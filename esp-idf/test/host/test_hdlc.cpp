/* Host test of tcp_hdlc.h, the HDLC codec and receive pump tcp.cpp runs,
 * against vectors from Python RNS's TCPInterface (hdlc_vectors.py).
 *
 * Encoder: hdlcEncode of every packet, concatenated, must equal RNS's framing
 * byte for byte.
 * Decoder: the stream goes through hdlcPumpRx exactly as pumpIn drives it — a
 * 512-byte held buffer, 8 rounds per call — with the reads split every way
 * that matters (1-byte reads, random sizes, a split right after every ESC, a
 * split on both sides of every FLAG) and with the sink holding frames the way
 * a full rnsd link does (randomly, or every frame once), each hold resumed by
 * a later call. Every packet must come out once, in order, byte for byte.
 *
 *   sh test_hdlc.sh            (needs g++ and the partyline venv's RNS)
 */
#include "tcp_hdlc.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <vector>

typedef std::vector<uint8_t> Bytes;

/* The fields tcp.cpp's peer_t gives hdlcPumpRx, at the firmware's sizes. */
struct Conn {
    uint8_t rx_pkt[RNS_MTU + 8];
    size_t  rx_len = 0;
    bool    rx_in_frame = false;
    bool    rx_escaping = false;
    uint8_t rx_buf[512];
    size_t  rx_off = 0;
    size_t  rx_fill = 0;
};

static Bytes readFile(const std::string& path)
{
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "cannot open %s\n", path.c_str()); exit(2); }
    Bytes b;
    uint8_t buf[65536];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) b.insert(b.end(), buf, buf + n);
    fclose(f);
    return b;
}

static int s_failures = 0;
#define CHECK(cond, ...) do { if (!(cond)) { s_failures++; fprintf(stderr, "FAIL: " __VA_ARGS__); fputc('\n', stderr); } } while (0)

struct Sink {
    std::vector<Bytes>* out;
    std::function<bool()> hold;   /* true: rnsd is full, hold this frame */
    size_t holds = 0, oversizes = 0;
    bool frame(const uint8_t* pkt, size_t len) {
        if (hold()) { holds++; return false; }
        out->push_back(Bytes(pkt, pkt + len));
        return true;
    }
    void oversize() { oversizes++; }
};

/* Run the whole stream through hdlcPumpRx; `cut(pos)` gives the size of the
 * net read starting at stream offset pos (clamped to the buffer's room). */
static std::vector<Bytes> decode(const Bytes& stream, std::function<size_t(size_t)> cut,
                                 std::function<bool()> hold, size_t* holdsOut)
{
    Conn c;
    std::vector<Bytes> out;
    Sink sink{&out, hold};
    size_t pos = 0;
    size_t calls = 0, stuck = 0;
    auto read = [&](uint8_t* buf, size_t cap) -> size_t {
        if (pos >= stream.size()) return 0;
        size_t n = cut(pos);
        if (n == 0) n = 1;
        if (n > cap) n = cap;
        if (n > stream.size() - pos) n = stream.size() - pos;
        memcpy(buf, stream.data() + pos, n);
        pos += n;
        return n;
    };
    /* The task loop: call again while bytes are held or unread. */
    while (pos < stream.size() || c.rx_off < c.rx_fill) {
        size_t before = pos + (c.rx_fill - c.rx_off), outBefore = out.size();
        hdlcPumpRx(&c, sink, read, [] {});
        calls++;
        size_t after = pos + (c.rx_fill - c.rx_off);
        stuck = (after == before && out.size() == outBefore) ? stuck + 1 : 0;
        if (stuck > 1000 || calls > 50000000) { fprintf(stderr, "no progress\n"); break; }
    }
    CHECK(sink.oversizes == 0, "%zu oversize frames", sink.oversizes);
    if (holdsOut) *holdsOut = sink.holds;
    return out;
}

static void compare(const char* name, const std::vector<Bytes>& got, const std::vector<Bytes>& want,
                    size_t holds)
{
    size_t bad = 0;
    for (size_t i = 0; i < want.size() && i < got.size(); i++) {
        if (got[i] != want[i]) {
            if (bad++ < 3)
                fprintf(stderr, "  %s: packet %zu: got %zu B, want %zu B\n", name, i,
                        got[i].size(), want[i].size());
        }
    }
    CHECK(got.size() == want.size() && bad == 0, "%s: %zu packets out (want %zu), %zu differ",
          name, got.size(), want.size(), bad);
    printf("%-28s %zu packets, %zu holds: %s\n", name, got.size(), holds,
           (got.size() == want.size() && bad == 0) ? "ok" : "MISMATCH");
}

int main(int argc, char** argv)
{
    if (argc < 2) { fprintf(stderr, "usage: %s VECTORDIR\n", argv[0]); return 2; }
    std::string dir = argv[1];
    Bytes pk = readFile(dir + "/packets.bin");
    Bytes frames = readFile(dir + "/frames.bin");
    Bytes stream = readFile(dir + "/stream.bin");

    std::vector<Bytes> packets;
    for (size_t i = 0; i + 2 <= pk.size();) {
        size_t n = pk[i] | (pk[i + 1] << 8);
        packets.push_back(Bytes(pk.begin() + i + 2, pk.begin() + i + 2 + n));
        i += 2 + n;
    }

    /* Encoder against RNS's framing. */
    Bytes enc;
    for (auto& p : packets) {
        uint8_t buf[HDLC_MAX_FRAME];
        size_t n = hdlcEncode(p.data(), p.size(), buf);
        CHECK(n <= HDLC_MAX_FRAME, "frame over HDLC_MAX_FRAME");
        enc.insert(enc.end(), buf, buf + n);
    }
    CHECK(enc == frames, "hdlcEncode differs from RNS framing (%zu vs %zu B)", enc.size(), frames.size());
    printf("%-28s %zu packets, %zu B: %s\n", "encode", packets.size(), enc.size(),
           enc == frames ? "ok" : "MISMATCH");

    std::mt19937 rng(12345);
    auto never = [] { return false; };
    auto randomHold = [&rng] { return (rng() % 100) < 30; };
    /* Every frame refused once, then taken — the stall-then-resume path on
     * every single frame. */
    bool heldLast = false;
    auto holdEachOnce = [&heldLast] { heldLast = !heldLast; return heldLast; };

    /* Next ESC-ended / FLAG-bounded cut from pos. */
    auto afterEsc = [](const Bytes& s) {
        return [&s](size_t pos) -> size_t {
            for (size_t i = pos; i < s.size(); i++) if (s[i] == HDLC_ESC) return i - pos + 1;
            return s.size() - pos;
        };
    };
    auto aroundFlag = [](const Bytes& s) {
        return [&s](size_t pos) -> size_t {
            if (s[pos] == HDLC_FLAG) return 1;
            for (size_t i = pos; i < s.size(); i++) if (s[i] == HDLC_FLAG) return i - pos;
            return s.size() - pos;
        };
    };

    const Bytes* inputs[2] = { &frames, &stream };
    const char* inputNames[2] = { "frames", "stream" };
    for (int k = 0; k < 2; k++) {
        const Bytes& s = *inputs[k];
        struct { const char* name; std::function<size_t(size_t)> cut; } cuts[] = {
            { "1B",       [](size_t) -> size_t { return 1; } },
            { "512B",     [](size_t) -> size_t { return 512; } },
            { "random",   [&rng](size_t) -> size_t { return 1 + rng() % 700; } },
            { "afterESC", afterEsc(s) },
            { "atFLAG",   aroundFlag(s) },
        };
        struct { const char* name; std::function<bool()> hold; } holds[] = {
            { "nohold", never }, { "randhold", randomHold }, { "holdonce", holdEachOnce },
        };
        for (auto& c : cuts) {
            for (auto& h : holds) {
                heldLast = false;
                size_t nh = 0;
                std::vector<Bytes> got = decode(s, c.cut, h.hold, &nh);
                std::string name = std::string(inputNames[k]) + "/" + c.name + "/" + h.name;
                compare(name.c_str(), got, packets, nh);
            }
        }
    }

    if (s_failures) { printf("FAILED (%d)\n", s_failures); return 1; }
    printf("PASS\n");
    return 0;
}
