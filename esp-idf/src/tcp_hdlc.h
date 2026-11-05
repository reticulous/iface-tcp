#pragma once
/**
 * tcp_hdlc — HDLC framing of RNS packets on the TCP byte stream, and the
 * receive pump that feeds the decoder from a held read buffer.
 *
 * The framing is RNS TCPInterface's: FLAG (0x7E) opens and closes a frame, and
 * a FLAG or ESC (0x7D) inside one is sent as ESC followed by the byte XOR
 * ESC_MASK (0x20). Bytes outside a frame are ignored, as are empty frames.
 *
 * Free of FreeRTOS, ITS and lwIP so the host test (test/host/test_hdlc.cpp)
 * runs the code tcp.cpp runs. Templated over the connection type: P carries
 * the assembly fields rx_pkt[], rx_len, rx_in_frame, rx_escaping and the held
 * read buffer rx_buf[], rx_off, rx_fill.
 */
#include <stddef.h>
#include <stdint.h>

#define HDLC_FLAG      0x7E
#define HDLC_ESC       0x7D
#define HDLC_ESC_MASK  0x20
#define RNS_MTU        500
#define HDLC_MAX_FRAME (RNS_MTU * 2 + 4)   /* every byte escaped + two FLAGs */

/* HDLC-frame a packet into buf (HDLC_MAX_FRAME bytes); returns the frame length. */
static inline size_t hdlcEncode(const uint8_t* data, size_t len, uint8_t* buf)
{
    size_t o = 0;
    buf[o++] = HDLC_FLAG;
    for (size_t i = 0; i < len && o < HDLC_MAX_FRAME - 2; i++) {
        uint8_t b = data[i];
        if (b == HDLC_FLAG || b == HDLC_ESC) {
            buf[o++] = HDLC_ESC;
            buf[o++] = (uint8_t)(b ^ HDLC_ESC_MASK);
        } else {
            buf[o++] = b;
        }
    }
    buf[o++] = HDLC_FLAG;
    return o;
}

/* Decode n stream bytes into p's assembly state. Each complete frame goes to
 * sink.frame(pkt, len), which returns false to hold it: decoding then stops
 * with the closing FLAG unread and the frame still assembled, and the return
 * value is the count of bytes consumed, so feeding the rest later (from that
 * FLAG) offers the same frame again. A frame over p->rx_pkt's size is
 * abandoned through sink.oversize(). Returns n when every byte was taken. */
template<typename P, typename Sink>
static size_t hdlcDecode(P* p, const uint8_t* in, size_t n, Sink& sink)
{
    for (size_t i = 0; i < n; i++) {
        uint8_t b = in[i];
        if (b == HDLC_FLAG) {
            if (p->rx_in_frame && p->rx_len > 0 && !sink.frame(p->rx_pkt, p->rx_len))
                return i;
            p->rx_len = 0;
            p->rx_in_frame = true;
            p->rx_escaping = false;
            continue;
        }
        if (!p->rx_in_frame) continue;
        if (b == HDLC_ESC) {
            p->rx_escaping = true;
            continue;
        }
        if (p->rx_escaping) {
            b ^= HDLC_ESC_MASK;
            p->rx_escaping = false;
        }
        if (p->rx_len < sizeof(p->rx_pkt)) {
            p->rx_pkt[p->rx_len++] = b;
        } else {
            p->rx_len = 0;
            p->rx_in_frame = false;
            p->rx_escaping = false;
            sink.oversize();
        }
    }
    return n;
}

/* Up to `rounds` chunks: decode the bytes held in rx_buf[rx_off..rx_fill);
 * once all are taken, refill rx_buf with read(buf, cap) (0 = nothing more) and
 * call between() so the other direction gets a turn after every chunk. Stops
 * early with the rest held when the sink holds a frame. */
template<typename P, typename Sink, typename Read, typename Between>
static void hdlcPumpRx(P* p, Sink& sink, Read read, Between between, int rounds = 8)
{
    for (int round = 0; round < rounds; round++) {
        if (p->rx_off < p->rx_fill) {
            p->rx_off += hdlcDecode(p, p->rx_buf + p->rx_off, p->rx_fill - p->rx_off, sink);
            if (p->rx_off < p->rx_fill) break;
        }
        p->rx_off = p->rx_fill = 0;
        size_t n = read(p->rx_buf, sizeof(p->rx_buf));
        if (n == 0) break;
        p->rx_fill = n;
        between();
    }
}
