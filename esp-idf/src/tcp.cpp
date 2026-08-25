/**
 * tcp — TCP interface task (outbound dial + inbound listen).
 *
 * Outbound: watches s.tcp.peers[] and for each enabled peer:
 *   1. Dials via net's NET_PORT_TCP_DIAL with "host:port" payload.
 *   2. On success, registers with rnsd via RNSD_PORT_IFACE as
 *      iface "tcp/<id>".
 *   3. Shuttles bytes both ways with HDLC framing on the net handle
 *      (FLAG=0x7E, ESC=0x7D, ESC_MASK=0x20).
 *   4. Reconnect with exponential backoff per s.tcp.peers[i].retry_*.
 *
 * Inbound: the s.tcp.servers[] collection (Incoming Ports) registers one TCP
 * listener with net per entry; each accepted client becomes its own rnsd
 * iface "tcp_in/<addr>#<slot>", framed identically.
 */
#include "tcp.h"
#include "rnsd.h"         /* rnsServiceRegister, rnsd_iface_t, RNSD_PORT_IFACE */
#include "spangap.h"
#include "mem.h"
#include "net.h"          /* netRegister, netIsUp, NET_EV_*, NET_PORT_TCP_DIAL */
#include "ports.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <map>

static const char* TAG = "tcp";

#define TCP_VERSION    3
#define TCP_MAX_PEERS  16
#define TCP_MAX_INBOUND 8        /* hard cap on concurrent inbound connections */
#define TCP_MAX_SERVERS 4        /* incoming-port listeners (s.tcp.servers) */
#define TCP_PORT_INBOUND 0x5443  /* ITS server port for accepted inbound conns */
#define HDLC_FLAG      0x7E
#define HDLC_ESC       0x7D
#define HDLC_ESC_MASK  0x20
#define RNS_MTU        500

enum peer_state_t : uint8_t {
    PS_IDLE = 0,
    PS_CONNECTING,
    PS_UP,
    PS_BACKOFF,
};

struct peer_t {
    int      id;             /* current array index (matches storage) */
    int      runtime_id;     /* stable across reloads — used as ITS ref so
                              * disconnect callbacks survive index shifts */
    bool     enabled;
    char     host[64];
    uint16_t port;
    uint8_t  mode;
    char     ifac_netname[32];   /* IFAC network_name (s.); "" = open */
    char     ifac_netkey[64];    /* IFAC passphrase; "" = open */
    uint8_t  ifac_size;          /* IFAC access-code length; 0 = default */
    uint8_t  announce_cap;       /* % bandwidth cap for announces; 0 = default */
    uint8_t  community_radius;   /* serve nodes within this many hops via this peer; 0 = none */
    uint32_t retry_min_s;
    uint32_t retry_max_s;
    uint32_t cur_backoff_s;

    peer_state_t state;
    int      net_handle;     /* ITS handle to net (TCP byte stream) */
    int      rnsd_handle;    /* ITS handle to rnsd (RNS packet stream) */
    TickType_t next_attempt_tick;

    /* HDLC inbound assembly */
    uint8_t  rx_pkt[RNS_MTU + 8];
    size_t   rx_len;
    bool     rx_in_frame;
    bool     rx_escaping;

    /* Coalesced rnsd-drop accounting — see noteRnsdDrop. */
    bool       rx_drop_open;    /* a report window is in progress */
    uint32_t   rx_drops;        /* frames dropped since the last line */
    uint32_t   rx_drop_bytes;   /* their total payload */
    TickType_t rx_drop_since;   /* tick the current window opened */

    uint64_t bytes_in;
    uint64_t bytes_out;
};

static std::vector<peer_t> s_peers;
static int s_next_runtime_id = 1;
static TaskHandle_t s_task = nullptr;
static volatile bool s_stop = false;   /* rns stop → break the task loop and park */
static volatile bool s_parked = false; /* true while parked (stopped); tcpStop waits on it */
static TickType_t s_nextPublishTick = 0;   /* throttles periodic stats publish to ~1 Hz */

static peer_t* peerByRuntimeId(int rid) {
    for (auto& p : s_peers) if (p.runtime_id == rid) return &p;
    return nullptr;
}

static peer_t* peerById(int id) {
    for (auto& p : s_peers) if (p.id == id) return &p;
    return nullptr;
}

/* A peer is dialable only if enabled AND fully addressed. An enabled peer
 * with an empty host or zero port can never connect: attemptConnect no-ops
 * on it (see its guard) without advancing next_attempt_tick or changing
 * state, so nextDeadline keeps returning 0 and itsPoll never blocks — the
 * task loop spins, starves IDLE0, and trips the task WDT shortly after boot.
 * Exclude such peers everywhere we decide to dial or compute the sleep. */
static inline bool peerDialable(const peer_t& p) {
    return p.enabled && p.host[0] != '\0' && p.port != 0;
}

/* Global TCP gate — `s.tcp.enable`. When false, all peer connections
 * are torn down regardless of per-peer enable. Tearing down + bringing
 * back up reconnects to rnsd as a side effect (each peer's
 * net_handle close → rnsd_handle close → rnsd.deregister_interface;
 * next dial re-registers a fresh iface). */
static bool s_globalEnable = true;

/* Upstream gate. A peer can only dial while we're connected to a real
 * upstream network as STA. AP-only mode has no route off-device, so net
 * rejects the dial (`!netIsStaConnected`) — and the doomed attempt still
 * spends a connect/teardown cycle, logs, and cycles the rnsd iface
 * register/deregister path. Worse, it keeps the tcp task waking on backoff
 * deadlines that can never succeed, defeating light sleep. We therefore track
 * the STA-upstream edge via NET_EV_UPSTREAM_UP/DOWN (NOT NET_EV_UP, which
 * also fires for AP-start) and only attempt connects while upstream is up; on
 * upstream-down peers are torn down once here, on upstream-up backoff is
 * cleared so they redial promptly. Written from the net task (edge events)
 * and read on the tcp task — volatile, single-word. */
static volatile bool s_upstreamUp = false;
static volatile bool s_netEdge    = false;   /* upstream just came up — clear backoff, redial now */

/* ─────────────── HDLC ─────────────── */

/* Write HDLC-framed packet to net handle. Returns true if all bytes sent. */
static bool hdlcSend(int netHandle, const uint8_t* data, size_t len)
{
    uint8_t buf[RNS_MTU * 2 + 4];
    size_t  o = 0;
    buf[o++] = HDLC_FLAG;
    for (size_t i = 0; i < len && o < sizeof(buf) - 2; i++) {
        uint8_t b = data[i];
        if (b == HDLC_FLAG || b == HDLC_ESC) {
            buf[o++] = HDLC_ESC;
            buf[o++] = (uint8_t)(b ^ HDLC_ESC_MASK);
        } else {
            buf[o++] = b;
        }
    }
    buf[o++] = HDLC_FLAG;
    size_t sent = itsSend(netHandle, buf, o, pdMS_TO_TICKS(500));
    return sent == o;
}

/* ── coalesced rnsd-drop reporting ──
 *
 * When rnsd's packet link fills, EVERY inbound frame drops, and a line per
 * dropped frame is enough work on this task to hold up the very task that
 * would clear the condition — rnsd shares core 0 with us, so every slice
 * spent formatting drop lines is a slice it does not get to drain the link
 * that is overflowing. Left unbounded the storm is self-sustaining, and it
 * shows up as a WDT trip on IDLE0 with this task caught inside the log call.
 * So: report the first drop of a run immediately, then one summary line per
 * window carrying how many followed. */
static const TickType_t RNSD_DROP_WINDOW = pdMS_TO_TICKS(5000);

/* Emit the accumulated tail of a window and restart it. */
template<typename P>
static void reportRnsdDrops(P* p, TickType_t now)
{
    warn("rnsd ITS send dropped: %u more frames (%u B) in %u ms",
         (unsigned)p->rx_drops, (unsigned)p->rx_drop_bytes,
         (unsigned)pdTICKS_TO_MS(now - p->rx_drop_since));
    p->rx_drops      = 0;
    p->rx_drop_bytes = 0;
    p->rx_drop_since = now;
}

/* Count one dropped frame. Tick arithmetic is unsigned throughout, so the
 * elapsed compare is wrap-safe and no field needs seeding beyond zero. */
template<typename P>
static void noteRnsdDrop(P* p, size_t n)
{
    TickType_t now = xTaskGetTickCount();
    if (!p->rx_drop_open) {
        warn("rnsd ITS send dropped (%zu B)", n);
        p->rx_drop_open  = true;
        p->rx_drops      = 0;
        p->rx_drop_bytes = 0;
        p->rx_drop_since = now;
        return;
    }
    p->rx_drops++;
    p->rx_drop_bytes += (uint32_t)n;
    if (now - p->rx_drop_since >= RNSD_DROP_WINDOW) reportRnsdDrops(p, now);
}

/* Close out a window whose drops stopped before it expired, so the tail is
 * never silently lost and the next drop prints promptly again. Called from
 * the task loop; `force` skips the window wait for teardown paths. */
template<typename P>
static void flushRnsdDrops(P* p, bool force = false)
{
    if (!p->rx_drop_open) return;
    TickType_t now = xTaskGetTickCount();
    if (!force && now - p->rx_drop_since < RNSD_DROP_WINDOW) return;
    if (p->rx_drops) reportRnsdDrops(p, now);
    else             p->rx_drop_open = false;
}

/* Decode bytes from net into the peer's pkt buffer. On a complete frame,
 * forward to rnsd as one ITS packet and reset assembly state. Templated so
 * both outbound peer_t and inbound_peer_t (identical HDLC fields) can use it. */
template<typename P>
static void hdlcConsume(P* p, const uint8_t* in, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        uint8_t b = in[i];
        if (b == HDLC_FLAG) {
            if (p->rx_in_frame && p->rx_len > 0) {
                /* End of frame — emit to rnsd. */
                if (p->rnsd_handle >= 0) {
                    size_t s = itsSend(p->rnsd_handle, p->rx_pkt, p->rx_len, pdMS_TO_TICKS(100));
                    if (s == 0) noteRnsdDrop(p, p->rx_len);
                }
            }
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
            /* Frame too big — abort assembly. */
            p->rx_len = 0;
            p->rx_in_frame = false;
            p->rx_escaping = false;
            warn("hdlc: frame > %d B, dropped", RNS_MTU);
        }
    }
}

/* ─────────────── peer config ─────────────── */

static void loadPeerConfig(peer_t& p, int id)
{
    char key[64];
    p.id = id;
    snprintf(key, sizeof(key), "s.tcp.peers.%d.enable", id);
    p.enabled = storageGetInt(key, 0) != 0;
    snprintf(key, sizeof(key), "s.tcp.peers.%d.host", id);
    storageGetStr(key, p.host, sizeof(p.host), "");
    snprintf(key, sizeof(key), "s.tcp.peers.%d.port", id);
    p.port = (uint16_t)storageGetInt(key, 4965);
    snprintf(key, sizeof(key), "s.tcp.peers.%d.mode", id);
    char mode[24] = "access_point";
    storageGetStr(key, mode, sizeof(mode), "access_point");
    if      (strcmp(mode, "full")         == 0) p.mode = RNS_IFACE_MODE_FULL;
    else if (strcmp(mode, "gateway")      == 0) p.mode = RNS_IFACE_MODE_GATEWAY;
    else if (strcmp(mode, "access_point") == 0) p.mode = RNS_IFACE_MODE_ACCESS_POINT;
    else if (strcmp(mode, "roaming")      == 0) p.mode = RNS_IFACE_MODE_ROAMING;
    else if (strcmp(mode, "boundary")     == 0) p.mode = RNS_IFACE_MODE_BOUNDARY;
    else                                        p.mode = RNS_IFACE_MODE_ACCESS_POINT;
    /* IFAC: both halves are ordinary fields of the item. The passphrase is
     * masked where it is shown and readable where it is asked for — it is the
     * code the other end of this link was given, and an operator who cannot
     * read back what they typed cannot tell why the link is silent. */
    snprintf(key, sizeof(key), "s.tcp.peers.%d.ifac_netname", id);
    storageGetStr(key, p.ifac_netname, sizeof(p.ifac_netname), "");
    snprintf(key, sizeof(key), "s.tcp.peers.%d.ifac_netkey", id);
    storageGetStr(key, p.ifac_netkey, sizeof(p.ifac_netkey), "");
    snprintf(key, sizeof(key), "s.tcp.peers.%d.ifac_size", id);
    p.ifac_size = (uint8_t)storageGetInt(key, 0);
    snprintf(key, sizeof(key), "s.tcp.peers.%d.announce_cap", id);
    p.announce_cap = (uint8_t)storageGetInt(key, RNS_IFACE_ANNOUNCE_CAP_DEFAULT);
    /* Radius 0 by default: a TCP peer into the wider network delivers the
     * announces of everyone, unbounded, and re-acquiring any of them costs
     * one path request over a cheap link — so nothing unrequested is stored
     * and no errands are run. Raise it for a peer that fronts a segment this
     * node should serve. */
    snprintf(key, sizeof(key), "s.tcp.peers.%d.community_radius", id);
    p.community_radius = (uint8_t)storageGetInt(key, 0);
    snprintf(key, sizeof(key), "s.tcp.peers.%d.retry_min", id);
    p.retry_min_s = (uint32_t)storageGetInt(key, 2);
    snprintf(key, sizeof(key), "s.tcp.peers.%d.retry_max", id);
    p.retry_max_s = (uint32_t)storageGetInt(key, 300);
    if (p.retry_min_s == 0) p.retry_min_s = 2;
    if (p.retry_max_s < p.retry_min_s) p.retry_max_s = p.retry_min_s;
}

static std::string peerField(int idx, const char* field);   /* the collection store, below */

static void publishPeerState(peer_t& p)
{
    char key[64];
    storageBegin();
    snprintf(key, sizeof(key), "tcp.peers.%d.up", p.id);    storageSet(key, p.state == PS_UP ? 1 : 0);
    snprintf(key, sizeof(key), "tcp.peers.%d.state", p.id);
    const char* stStr = "idle";
    switch (p.state) {
        case PS_IDLE:       stStr = "idle";       break;
        case PS_CONNECTING: stStr = "connecting"; break;
        case PS_UP:         stStr = "up";         break;
        case PS_BACKOFF:    stStr = "backoff";    break;
    }
    storageSet(key, stStr);
    /* The settings collection's status pill, as packed "text|colour". Which
     * words and which colour a peer state deserves is a judgement about this
     * interface, so it is made here rather than by each surface. */
    std::string id = peerField(p.id, "id");
    if (!id.empty()) {
        const char* pill = "";
        switch (p.state) {
            case PS_UP:         pill = "up|green";          break;
            case PS_CONNECTING: pill = "connecting|amber";  break;
            case PS_BACKOFF:    pill = "backoff|red";       break;
            case PS_IDLE:       pill = p.enabled ? "idle|grey" : "off|grey"; break;
        }
        snprintf(key, sizeof(key), "tcp.peer.%s", id.c_str());
        storageSet(key, pill);
    }
    snprintf(key, sizeof(key), "tcp.peers.%d.stats.tx_bytes", p.id); storageSet(key, (int)(p.bytes_out & 0x7fffffff));
    snprintf(key, sizeof(key), "tcp.peers.%d.stats.rx_bytes", p.id); storageSet(key, (int)(p.bytes_in  & 0x7fffffff));
    storageEnd();
}

/* ─────────────── connection lifecycle ─────────────── */

static void disconnectPeer(peer_t& p, const char* reason)
{
    if (p.rnsd_handle >= 0) { itsDisconnect(p.rnsd_handle); p.rnsd_handle = -1; }
    if (p.net_handle  >= 0) { itsDisconnect(p.net_handle);  p.net_handle  = -1; }
    flushRnsdDrops(&p, /*force=*/true);
    p.rx_drop_open = false;
    p.rx_len = 0;
    p.rx_in_frame = false;
    p.rx_escaping = false;
    if (p.state == PS_UP || p.state == PS_CONNECTING) {
        info("peer[%d] %s:%u disconnect: %s", p.id, p.host, (unsigned)p.port, reason);
    }
    p.state = PS_BACKOFF;
    if (p.cur_backoff_s == 0) p.cur_backoff_s = p.retry_min_s;
    else                      p.cur_backoff_s = (p.cur_backoff_s * 2 <= p.retry_max_s)
                                                  ? p.cur_backoff_s * 2
                                                  : p.retry_max_s;
    p.next_attempt_tick = xTaskGetTickCount() + pdMS_TO_TICKS(p.cur_backoff_s * 1000);
    publishPeerState(p);
}

static void onNetRecv(int handle, size_t /*bytesAvail*/);
static void onNetDisconnect(int ref);
static void onRnsdRecv(int handle, size_t /*bytesAvail*/);
static void onRnsdDisconnect(int ref);

static peer_t* peerByNetHandle(int h) {
    for (auto& p : s_peers) if (p.net_handle == h) return &p;
    return nullptr;
}
static peer_t* peerByRnsdHandle(int h) {
    for (auto& p : s_peers) if (p.rnsd_handle == h) return &p;
    return nullptr;
}

static void onNetRecv(int handle, size_t /*bytesAvail*/) {
    /* Plain static — ITS dispatches recv callbacks only on the task that
     * registered them (tcp task here), so no concurrency. Avoid
     * `thread_local` which on ESP-IDF/libgcc pulls in lazy TLS init that
     * has been seen to corrupt FreeRTOS scheduler state at boot. */
    PSRAM_BSS static uint8_t buf[1024];
    /* Always drain first, then decide what to do with it. Returning before
     * the itsRecv would leave the bytes in the buffer; ITS would keep
     * redispatching this callback with nothing consumed → busy spin (WDT). */
    size_t n = itsRecv(handle, buf, sizeof(buf), 0);
    if (n == 0) return;
    peer_t* p = peerByNetHandle(handle);
    if (!p) return;   /* conn outlived its peer — drop the drained bytes */
    p->bytes_in += n;
    hdlcConsume(p, buf, n);
}
static void onNetDisconnect(int ref) {
    peer_t* p = peerByRuntimeId(ref);
    if (!p) return;
    p->net_handle = -1;
    disconnectPeer(*p, "net closed");
}
static void onRnsdRecv(int handle, size_t /*bytesAvail*/) {
    PSRAM_BSS static uint8_t pkt[RNS_MTU + 16];
    /* Drain first, then decide. rnsd floods this iface with the network's
     * announce stream; if we returned before itsRecv whenever the net side
     * is momentarily down, the packet would sit in the buffer and ITS would
     * redispatch us forever with nothing consumed → busy spin (WDT). */
    size_t n = itsRecv(handle, pkt, sizeof(pkt), 0);
    if (n == 0) return;
    peer_t* p = peerByRnsdHandle(handle);
    if (!p || p->net_handle < 0) return;   /* can't forward — drop drained pkt */
    if (!hdlcSend(p->net_handle, pkt, n)) {
        warn("peer[%d] hdlc send failed", p->id);
        return;
    }
    p->bytes_out += n;
}
static void onRnsdDisconnect(int ref) {
    peer_t* p = peerByRuntimeId(ref);
    if (!p) return;
    p->rnsd_handle = -1;
    disconnectPeer(*p, "rnsd closed");
}

static void attemptConnect(peer_t& p)
{
    if (!p.enabled || !s_upstreamUp || p.host[0] == '\0' || p.port == 0) return;

    p.state = PS_CONNECTING;
    publishPeerState(p);
    info("peer[%d] dial %s:%u", p.id, p.host, (unsigned)p.port);

    char hp[80];
    snprintf(hp, sizeof(hp), "%s:%u", p.host, (unsigned)p.port);
    p.net_handle = itsConnect("net", NET_PORT_TCP_DIAL, hp, strlen(hp),
                              pdMS_TO_TICKS(12000), p.runtime_id, onNetRecv, onNetDisconnect);
    if (p.net_handle < 0) {
        disconnectPeer(p, "dial failed");
        return;
    }

    rnsd_iface_t reg = {};
    snprintf(reg.name, sizeof(reg.name), "tcp/%d", p.id);
    reg.mtu     = RNS_MTU;
    reg.bitrate = 1000000;  /* 1 Mbps — feeds RNS first-hop link timeout */
    reg.mode    = p.mode;
    reg.in = reg.out = 1;
    reg.fwd = (p.mode == RNS_IFACE_MODE_GATEWAY || p.mode == RNS_IFACE_MODE_FULL) ? 1 : 0;
    reg.rpt = 0;
    reg.ifac_size = p.ifac_size;
    reg.announce_cap = p.announce_cap;
    reg.point_to_point = 1;   /* one peer per TCP link — no hidden nodes */
    reg.community_radius = p.community_radius;
    safeStrncpy(reg.ifac_netname, p.ifac_netname, sizeof(reg.ifac_netname));
    safeStrncpy(reg.ifac_netkey,  p.ifac_netkey,  sizeof(reg.ifac_netkey));

    p.rnsd_handle = itsConnect("rnsd", RNSD_PORT_IFACE, &reg, sizeof(reg),
                               pdMS_TO_TICKS(500), p.runtime_id, onRnsdRecv, onRnsdDisconnect);
    if (p.rnsd_handle < 0) {
        warn("peer[%d] rnsd register failed", p.id);
        disconnectPeer(p, "register failed");
        return;
    }

    p.state = PS_UP;
    p.cur_backoff_s = 0;
    publishPeerState(p);
    info("peer[%d] up as iface tcp/%d", p.id, p.id);
}

/* ─────────────── task loop ─────────────── */

static TickType_t nextDeadline(void)
{
    /* Nothing dials while upstream is down OR the global gate is off; sleep
     * until an upstream-up / config / enable / cmd notify wakes us (no 1 Hz
     * stats churn either — peer state is static, so the chip can stay in light
     * sleep). This MUST match servicePeers' dial gate: if we returned a finite
     * (here 0) deadline for an enabled peer that servicePeers won't actually
     * dial — e.g. an idle peer whose attempt tick is already past while the
     * global gate is off — itsPoll(0) would return instantly every loop,
     * spinning the task and starving IDLE0 → task WDT. */
    if (!s_upstreamUp || !s_globalEnable) return portMAX_DELAY;
    TickType_t now = xTaskGetTickCount();
    TickType_t soonest = portMAX_DELAY;
    for (auto& p : s_peers) {
        if (!peerDialable(p)) continue;
        if (p.state == PS_BACKOFF || p.state == PS_IDLE) {
            TickType_t d = (p.next_attempt_tick > now) ? (p.next_attempt_tick - now) : 0;
            if (d < soonest) soonest = d;
        }
    }
    /* Cap at 1s so we publish stats updates regularly even if idle. */
    TickType_t maxIdle = pdMS_TO_TICKS(1000);
    return soonest < maxIdle ? soonest : maxIdle;
}

static void servicePeers(void)
{
    TickType_t now = xTaskGetTickCount();
    for (auto& p : s_peers) {
        bool should_be_up = s_globalEnable && peerDialable(p) && s_upstreamUp;
        if (!should_be_up) {
            if (p.state == PS_UP || p.state == PS_CONNECTING)
                disconnectPeer(p, !s_upstreamUp   ? "upstream down"
                                : !s_globalEnable ? "tcp stopped"
                                                  : "disabled");
            continue;
        }
        if (p.state == PS_IDLE || p.state == PS_BACKOFF) {
            if ((int32_t)(now - p.next_attempt_tick) >= 0) attemptConnect(p);
        }
    }
}

static volatile bool s_configDirty = true;

static void onCfgChange(const char* /*key*/, const char* /*val*/) {
    s_configDirty = true;
    if (s_task) xTaskNotifyGive(s_task);
}

static void reloadPeers(void) {
    int newCount = storageArrayCount("s.tcp.peers");
    if (newCount > TCP_MAX_PEERS) {
        warn("config has %d peers; capping at %d", newCount, TCP_MAX_PEERS);
        newCount = TCP_MAX_PEERS;
    }

    /* Read new desired endpoints — host:port is the peer's identity. */
    struct desired_t { char host[sizeof(peer_t::host)]; uint16_t port; };
    std::vector<desired_t> desired(newCount);
    for (int i = 0; i < newCount; i++) {
        char key[64];
        snprintf(key, sizeof(key), "s.tcp.peers.%d.host", i);
        storageGetStr(key, desired[i].host, sizeof(desired[i].host), "");
        snprintf(key, sizeof(key), "s.tcp.peers.%d.port", i);
        desired[i].port = (uint16_t)storageGetInt(key, 4965);
        /* Self-heal the derived display title (Name, else host:port) so an
         * entry written before the field existed still renders one. */
        snprintf(key, sizeof(key), "s.tcp.peers.%d.display", i);
        char disp[80] = "";
        storageGetStr(key, disp, sizeof(disp), "");
        if (!disp[0]) {
            char namebuf[64] = "";
            char nk[64];
            snprintf(nk, sizeof(nk), "s.tcp.peers.%d.name", i);
            storageGetStr(nk, namebuf, sizeof(namebuf), "");
            if (namebuf[0]) snprintf(disp, sizeof(disp), "%s", namebuf);
            else snprintf(disp, sizeof(disp), "%s:%u", desired[i].host, (unsigned)desired[i].port);
            storageSet(key, disp);
        }
    }

    /* Build new vector by matching old peers to desired entries by host:port,
     * preserving their connection state. Unmatched old peers are disconnected. */
    std::vector<peer_t> next;
    next.reserve(newCount);
    std::vector<bool> oldMatched(s_peers.size(), false);

    for (int i = 0; i < newCount; i++) {
        int oldIdx = -1;
        for (size_t j = 0; j < s_peers.size(); j++) {
            if (oldMatched[j]) continue;
            if (s_peers[j].port == desired[i].port &&
                strcmp(s_peers[j].host, desired[i].host) == 0) {
                oldIdx = (int)j;
                break;
            }
        }
        if (oldIdx >= 0) {
            peer_t p = s_peers[oldIdx];
            bool wasEnabled = p.enabled;
            /* mode + IFAC are baked into the rnsd registration at dial time, so a
             * change to any of them only takes effect on reconnect — capture the
             * old values and force a redial below if they moved. */
            uint8_t oldMode = p.mode;
            uint8_t oldIfacSize = p.ifac_size;
            uint8_t oldAnnounceCap = p.announce_cap;
            uint8_t oldRadius = p.community_radius;
            char oldNetname[sizeof(p.ifac_netname)]; safeStrncpy(oldNetname, p.ifac_netname, sizeof(oldNetname));
            char oldNetkey[sizeof(p.ifac_netkey)];   safeStrncpy(oldNetkey,  p.ifac_netkey,  sizeof(oldNetkey));
            loadPeerConfig(p, i);            /* refreshes all fields including id */
            bool settingsChanged = p.mode != oldMode || p.ifac_size != oldIfacSize ||
                p.announce_cap != oldAnnounceCap || p.community_radius != oldRadius ||
                strcmp(p.ifac_netname, oldNetname) != 0 || strcmp(p.ifac_netkey, oldNetkey) != 0;
            if (wasEnabled && !p.enabled) {
                disconnectPeer(p, "disabled");
                p.state = PS_IDLE;
                p.cur_backoff_s = 0;
            } else if (!wasEnabled && p.enabled) {
                p.state = PS_IDLE;
                p.cur_backoff_s = 0;
                p.next_attempt_tick = xTaskGetTickCount();
            } else if (settingsChanged && (p.state == PS_UP || p.state == PS_CONNECTING)) {
                /* Reconnect so the new mode/IFAC is applied to the rnsd iface. */
                disconnectPeer(p, "settings changed");
                p.state = PS_IDLE;
                p.cur_backoff_s = 0;
                p.next_attempt_tick = xTaskGetTickCount();
            }
            next.push_back(p);
            oldMatched[oldIdx] = true;
        } else {
            peer_t np = {};
            np.runtime_id = s_next_runtime_id++;
            np.net_handle = -1;
            np.rnsd_handle = -1;
            loadPeerConfig(np, i);
            np.state = PS_IDLE;
            np.cur_backoff_s = 0;
            np.next_attempt_tick = xTaskGetTickCount();
            next.push_back(np);
        }
    }

    /* Disconnect any old peers that didn't survive. */
    for (size_t j = 0; j < s_peers.size(); j++) {
        if (!oldMatched[j]) disconnectPeer(s_peers[j], "removed");
    }

    s_peers = std::move(next);
}

/* ─────────────── command handlers (sentinels) ───────────────
 *
 * Subscriptions are installed in tcpTaskMain so callbacks run on the
 * tcp task — same task that owns s_peers and reaches into mailbox
 * state. Each handler clears its sentinel at the end and ignores the
 * self-unset re-fire (val=""), same pattern as lxmf's cmd handlers. */

static void onGlobalEnableChange(const char* /*key*/, const char* val)
{
    bool enabled = !val || std::atoi(val) != 0;   /* missing/empty → on */
    if (enabled == s_globalEnable) return;
    s_globalEnable = enabled;
    info("tcp: globally %s", s_globalEnable ? "enabled" : "disabled");
    s_configDirty = true;
    if (s_task) xTaskNotifyGive(s_task);
}

static void onCmdConnect(const char* key, const char* val)
{
    if (!val || !*val) return;   /* self-unset re-fire */
    int n = std::atoi(val);
    storageUnset(key);
    peer_t* p = peerById(n);
    if (!p) { warn("tcp connect: no peer at slot %d", n); return; }
    if (p->state == PS_UP || p->state == PS_CONNECTING)
        disconnectPeer(*p, "reconnect");
    p->cur_backoff_s     = 0;
    p->next_attempt_tick = xTaskGetTickCount();
    p->state             = PS_IDLE;
    info("tcp: connect %d (%s:%u)", n, p->host, (unsigned)p->port);
    /* attemptConnect runs from servicePeers; let the main loop do it
     * on its next tick so all peer state transitions go through the
     * same path. */
    s_configDirty = true;
    if (s_task) xTaskNotifyGive(s_task);
}

static void onCmdDisconnect(const char* key, const char* val)
{
    if (!val || !*val) return;
    int n = std::atoi(val);
    storageUnset(key);
    peer_t* p = peerById(n);
    if (!p) { warn("tcp disconnect: no peer at slot %d", n); return; }
    if (p->state == PS_UP || p->state == PS_CONNECTING) {
        info("tcp: disconnect %d (%s:%u)", n, p->host, (unsigned)p->port);
        disconnectPeer(*p, "user");
    } else {
        cliPrintf("(slot %d already not connected)\n", n);
    }
}

static void onCmdRestart(const char* key, const char* val)
{
    if (!val || !*val) return;
    storageUnset(key);
    info("tcp: restart — tearing down all peer connections");
    for (auto& p : s_peers) {
        if (p.state == PS_UP || p.state == PS_CONNECTING)
            disconnectPeer(p, "restart");
        p.cur_backoff_s     = 0;
        p.next_attempt_tick = xTaskGetTickCount();
        p.state             = PS_IDLE;
    }
    /* servicePeers redials enabled peers on next tick. */
    s_configDirty = true;
    if (s_task) xTaskNotifyGive(s_task);
}

/* Remove peer `slot`. s.tcp.peers is a JSON array, so unsetting the whole
 * element (its patch value becomes null) deletes it AND shifts the rest down —
 * see deepMergeIntoArray in storage.cpp — and fires the s.tcp.peers subscription,
 * so the tcp task reloads and every UI (on-device + web) refreshes. Every field
 * of the peer, IFAC passphrase included, is in that element and goes with it. */
static void onCmdDel(const char* key, const char* val)
{
    if (!val || !*val) return;   /* self-unset re-fire */
    int slot = std::atoi(val);
    storageUnset(key);
    char k[64];
    storageBegin();
    std::snprintf(k, sizeof k, "s.tcp.peers.%d", slot); storageUnset(k);
    storageEnd();
    info("tcp: removed peer slot %d", slot);
}

/* ---- the peers collection ----
 *
 * The settings surfaces never write s.tcp.peers. They write tcp.peer.add /
 * .remove / .set / .order and this file applies them, so one description drives
 * both surfaces and validation lives in one place. A rejection is a sentence on
 * tcp.peer.error.
 *
 * `id` is a small number handed out on add and carried by the item forever: the
 * array is compacted on delete (reloadPeers reads it contiguously and matches
 * old connections by host:port), so a slot index would name a different peer
 * after every removal. */

static const char* const PEER_FIELDS[] =
    { "id", "name", "enable", "host", "port", "mode",
      "ifac_netname", "ifac_netkey",
      "announce_cap", "community_radius",
      "retry_min", "retry_max" };

static std::string peerField(int idx, const char* field)
{
    char k[80];
    std::snprintf(k, sizeof k, "s.tcp.peers.%d.%s", idx, field);
    return storageGetStr(k, "");
}

static int peerIndexOfId(const std::string& id)
{
    if (id.empty()) return -1;
    int n = storageArrayCount("s.tcp.peers");
    for (int i = 0; i < n; i++) if (peerField(i, "id") == id) return i;
    return -1;
}

static std::string peerNextId()
{
    int best = 0, n = storageArrayCount("s.tcp.peers");
    for (int i = 0; i < n; i++) {
        int v = std::atoi(peerField(i, "id").c_str());
        if (v > best) best = v;
    }
    char buf[12];
    std::snprintf(buf, sizeof buf, "%d", best + 1);
    return buf;
}

static void peerError(const char* why) { storageSet("tcp.peer.error", why); }

/** Accepted-mutation ack, shared by every tcp.peer.* sentinel: the open form
 *  closes when this moves. A monotonic per-boot counter, not a read-increment —
 *  reads see the committed tree, and the actor may not have applied the
 *  previous bump yet. */
static void peerAck()
{
    static int ack = 0;
    storageSet("tcp.peer.done", ++ack);
}

/** What is wrong with this peer, or "" if nothing is. The one place that
 *  decides — the add form and the item editor both land here. */
static std::string peerRejection(const std::string& host, const std::string& portStr)
{
    if (host.empty()) return "A peer needs a host.";
    if (host.find(' ') != std::string::npos) return "A host has no spaces in it.";
    int port = portStr.empty() ? 4965 : std::atoi(portStr.c_str());
    if (port <= 0 || port > 65535) return "A port is between 1 and 65535.";
    return "";
}

static void peerWrite(int idx, const std::string& id,
                      const std::map<std::string, std::string>& f)
{
    char k[80];
    std::snprintf(k, sizeof k, "s.tcp.peers.%d.id", idx);
    storageSet(k, id.c_str());
    for (const char* field : PEER_FIELDS) {
        if (std::strcmp(field, "id") == 0) continue;
        std::snprintf(k, sizeof k, "s.tcp.peers.%d.%s", idx, field);
        auto it = f.find(field);
        if (it == f.end() || it->second.empty()) storageUnset(k);
        else                                     storageSet(k, it->second.c_str());
    }
    /* The list's display title, derived here so both surfaces render the same
     * fallback: the Name field when one is set, host:port otherwise. */
    auto fv = [&](const char* field) {
        auto it = f.find(field);
        return it != f.end() ? it->second : peerField(idx, field);
    };
    std::string display = fv("name");
    if (display.empty()) display = fv("host") + ":" + fv("port");
    std::snprintf(k, sizeof k, "s.tcp.peers.%d.display", idx);
    storageSet(k, display.c_str());
}

static std::map<std::string, std::string> peerParse(const char* json, std::string* idOut)
{
    std::map<std::string, std::string> out;
    cJSON* o = cJSON_Parse(json);
    if (!o) return out;
    for (const char* f : PEER_FIELDS) {
        cJSON* m = cJSON_GetObjectItem(o, f);
        if (cJSON_IsString(m)) out[f] = m->valuestring;
    }
    cJSON* id = cJSON_GetObjectItem(o, "_id");
    if (idOut && cJSON_IsString(id)) *idOut = id->valuestring;
    cJSON_Delete(o);
    return out;
}

static void onPeerAdd(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string payload = val;
    storageUnset(key);
    auto f = peerParse(payload.c_str(), nullptr);
    std::string why = peerRejection(f["host"], f["port"]);
    if (!why.empty()) { peerError(why.c_str()); return; }
    int n = storageArrayCount("s.tcp.peers");
    if (n >= TCP_MAX_PEERS) { peerError("No free peer slot."); return; }
    if (f["port"].empty())   f["port"]   = "4965";
    if (f["mode"].empty())   f["mode"]   = "access_point";
    if (f["enable"].empty()) f["enable"] = "1";
    std::string id = peerNextId();
    storageBegin();
    peerWrite(n, id, f);
    peerError("");
    storageEnd();
    peerAck();
    info("tcp: added peer %s:%s at %d", f["host"].c_str(), f["port"].c_str(), n);
}

static void onPeerSet(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string payload = val;
    storageUnset(key);
    std::string id;
    auto f = peerParse(payload.c_str(), &id);
    int idx = peerIndexOfId(id);
    if (idx < 0) { peerError("That peer is no longer configured."); return; }
    std::string why = peerRejection(f["host"], f["port"]);
    if (!why.empty()) { peerError(why.c_str()); return; }
    storageBegin();
    peerWrite(idx, id, f);
    peerError("");
    storageEnd();
    peerAck();
}

/** Drop a peer, compacting the array so reloadPeers still sees it contiguous. */
static void onPeerRemove(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string id = val;
    storageUnset(key);
    int idx = peerIndexOfId(id), n = storageArrayCount("s.tcp.peers");
    if (idx < 0) { peerError("That peer is no longer configured."); return; }
    storageBegin();
    for (int i = idx; i < n - 1; i++) {
        std::map<std::string, std::string> f;
        for (const char* field : PEER_FIELDS)
            if (std::strcmp(field, "id") != 0) f[field] = peerField(i + 1, field);
        peerWrite(i, peerField(i + 1, "id"), f);
    }
    char tail[64];
    std::snprintf(tail, sizeof tail, "s.tcp.peers.%d", n - 1);
    storageDeleteTree(tail);
    peerError("");
    storageEnd();
    peerAck();
    info("tcp: removed peer %s", id.c_str());
}

/** An id order applied as a PREFERENCE PERMUTATION: recognized ids move into the
 *  stated relative order, unknown ids are ignored, unmentioned ids keep their
 *  place — so a drag is idempotent and survives a racing add or delete. */
static void onPeerOrder(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string csv = val;
    storageUnset(key);
    int n = storageArrayCount("s.tcp.peers");
    if (n <= 1) return;
    std::vector<std::string> wanted;
    for (size_t pos = 0; pos <= csv.size(); ) {
        size_t comma = csv.find(',', pos);
        wanted.push_back(csv.substr(pos, comma == std::string::npos ? comma : comma - pos));
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    std::vector<std::map<std::string, std::string>> items(n);
    std::vector<std::string> ids(n);
    for (int i = 0; i < n; i++) {
        ids[i] = peerField(i, "id");
        for (const char* f : PEER_FIELDS)
            if (std::strcmp(f, "id") != 0) items[i][f] = peerField(i, f);
    }
    std::vector<int> slots, order;
    for (int i = 0; i < n; i++)
        for (const std::string& w : wanted)
            if (ids[i] == w) { slots.push_back(i); break; }
    for (const std::string& w : wanted)
        for (int i = 0; i < n; i++)
            if (ids[i] == w) { order.push_back(i); break; }
    if (slots.size() != order.size() || slots.empty()) return;
    storageBegin();
    for (size_t s = 0; s < slots.size(); s++)
        peerWrite(slots[s], ids[order[s]], items[order[s]]);
    peerError("");
    storageEnd();
    peerAck();
}

/* ---- the Incoming Ports collection (s.tcp.servers) ----
 *
 * Same shape as the peers collection: the surfaces write tcp.server.add /
 * .remove / .set / .order, this file applies them, and a rejection is a
 * sentence on tcp.server.error. `id` is handed out on add and carried by the
 * item forever; the array is compacted on delete. */

static const char* const SERVER_FIELDS[] =
    { "id", "enable", "port", "mode", "max_conns",
      "community_radius", "ifac_netname", "ifac_netkey", "announce_cap" };

static std::string srvField(int idx, const char* field)
{
    char k[80];
    std::snprintf(k, sizeof k, "s.tcp.servers.%d.%s", idx, field);
    return storageGetStr(k, "");
}

static int srvIndexOfId(const std::string& id)
{
    if (id.empty()) return -1;
    int n = storageArrayCount("s.tcp.servers");
    for (int i = 0; i < n; i++) if (srvField(i, "id") == id) return i;
    return -1;
}

static std::string srvNextId()
{
    int best = 0, n = storageArrayCount("s.tcp.servers");
    for (int i = 0; i < n; i++) {
        int v = std::atoi(srvField(i, "id").c_str());
        if (v > best) best = v;
    }
    char buf[12];
    std::snprintf(buf, sizeof buf, "%d", best + 1);
    return buf;
}

static void srvError(const char* why) { storageSet("tcp.server.error", why); }

static void srvAck()
{
    static int ack = 0;
    storageSet("tcp.server.done", ++ack);
}

/** What is wrong with this server, or "" if nothing is. `exceptId` skips the
 *  item being edited in the duplicate-port check. */
static std::string srvRejection(const std::string& portStr, const std::string& exceptId)
{
    int port = portStr.empty() ? 4965 : std::atoi(portStr.c_str());
    if (port <= 0 || port > 65535) return "A port is between 1 and 65535.";
    int n = storageArrayCount("s.tcp.servers");
    for (int i = 0; i < n; i++) {
        if (!exceptId.empty() && srvField(i, "id") == exceptId) continue;
        if (std::atoi(srvField(i, "port").c_str()) == port)
            return "That port already has a listener.";
    }
    return "";
}

static void srvWrite(int idx, const std::string& id,
                     const std::map<std::string, std::string>& f)
{
    char k[80];
    std::snprintf(k, sizeof k, "s.tcp.servers.%d.id", idx);
    storageSet(k, id.c_str());
    for (const char* field : SERVER_FIELDS) {
        if (std::strcmp(field, "id") == 0) continue;
        std::snprintf(k, sizeof k, "s.tcp.servers.%d.%s", idx, field);
        auto it = f.find(field);
        if (it == f.end() || it->second.empty()) storageUnset(k);
        else                                     storageSet(k, it->second.c_str());
    }
}

static std::map<std::string, std::string> srvParse(const char* json, std::string* idOut)
{
    std::map<std::string, std::string> out;
    cJSON* o = cJSON_Parse(json);
    if (!o) return out;
    for (const char* f : SERVER_FIELDS) {
        cJSON* m = cJSON_GetObjectItem(o, f);
        if (cJSON_IsString(m)) out[f] = m->valuestring;
    }
    cJSON* id = cJSON_GetObjectItem(o, "_id");
    if (idOut && cJSON_IsString(id)) *idOut = id->valuestring;
    cJSON_Delete(o);
    return out;
}

static void onSrvAdd(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string payload = val;
    storageUnset(key);
    auto f = srvParse(payload.c_str(), nullptr);
    std::string why = srvRejection(f["port"], "");
    if (!why.empty()) { srvError(why.c_str()); return; }
    int n = storageArrayCount("s.tcp.servers");
    if (n >= TCP_MAX_SERVERS) { srvError("No free listener slot."); return; }
    if (f["port"].empty())   f["port"]   = "4965";
    if (f["mode"].empty())   f["mode"]   = "access_point";
    if (f["enable"].empty()) f["enable"] = "1";
    std::string id = srvNextId();
    storageBegin();
    srvWrite(n, id, f);
    srvError("");
    storageEnd();
    srvAck();
    info("tcp: added incoming port %s at %d", f["port"].c_str(), n);
}

static void onSrvSet(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string payload = val;
    storageUnset(key);
    std::string id;
    auto f = srvParse(payload.c_str(), &id);
    int idx = srvIndexOfId(id);
    if (idx < 0) { srvError("That listener is no longer configured."); return; }
    std::string why = srvRejection(f["port"], id);
    if (!why.empty()) { srvError(why.c_str()); return; }
    storageBegin();
    srvWrite(idx, id, f);
    srvError("");
    storageEnd();
    srvAck();
}

static void onSrvRemove(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string id = val;
    storageUnset(key);
    int idx = srvIndexOfId(id), n = storageArrayCount("s.tcp.servers");
    if (idx < 0) { srvError("That listener is no longer configured."); return; }
    storageBegin();
    for (int i = idx; i < n - 1; i++) {
        std::map<std::string, std::string> f;
        for (const char* field : SERVER_FIELDS)
            if (std::strcmp(field, "id") != 0) f[field] = srvField(i + 1, field);
        srvWrite(i, srvField(i + 1, "id"), f);
    }
    char tail[64];
    std::snprintf(tail, sizeof tail, "s.tcp.servers.%d", n - 1);
    storageDeleteTree(tail);
    srvError("");
    storageEnd();
    srvAck();
    info("tcp: removed incoming port %s", id.c_str());
}

static void onSrvOrder(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string csv = val;
    storageUnset(key);
    int n = storageArrayCount("s.tcp.servers");
    if (n <= 1) return;
    std::vector<std::string> wanted;
    for (size_t pos = 0; pos <= csv.size(); ) {
        size_t comma = csv.find(',', pos);
        wanted.push_back(csv.substr(pos, comma == std::string::npos ? comma : comma - pos));
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    std::vector<std::map<std::string, std::string>> items(n);
    std::vector<std::string> ids(n);
    for (int i = 0; i < n; i++) {
        ids[i] = srvField(i, "id");
        for (const char* f : SERVER_FIELDS)
            if (std::strcmp(f, "id") != 0) items[i][f] = srvField(i, f);
    }
    std::vector<int> slots, order;
    for (int i = 0; i < n; i++)
        for (const std::string& w : wanted)
            if (ids[i] == w) { slots.push_back(i); break; }
    for (const std::string& w : wanted)
        for (int i = 0; i < n; i++)
            if (ids[i] == w) { order.push_back(i); break; }
    if (slots.size() != order.size() || slots.empty()) return;
    storageBegin();
    for (size_t s = 0; s < slots.size(); s++)
        srvWrite(slots[s], ids[order[s]], items[order[s]]);
    srvError("");
    storageEnd();
    srvAck();
}

/** Connect the peer with this id. wifi-style: the collection knows ids, the
 *  existing tcp.cmd.connect sentinel knows slots, so translate here. */
static void onPeerConnect(const char* key, const char* val)
{
    if (!val || !*val) return;
    std::string id = val;
    storageUnset(key);
    int idx = peerIndexOfId(id);
    if (idx >= 0) storageSet("tcp.cmd.connect", idx);
}

/** Give every peer the id the settings collection addresses it by, for a store
 *  written before ids existed. Idempotent. */
static void peerEnsureIds(void)
{
    int n = storageArrayCount("s.tcp.peers");
    bool any = false;
    for (int i = 0; i < n; i++) if (peerField(i, "id").empty()) { any = true; break; }
    if (!any) return;
    storageBegin();
    for (int i = 0; i < n; i++) {
        if (!peerField(i, "id").empty()) continue;
        char k[80], v[12];
        std::snprintf(v, sizeof v, "%d", i + 1);
        std::snprintf(k, sizeof k, "s.tcp.peers.%d.id", i);
        storageSet(k, v);
    }
    storageEnd();
}

/* ─────────────── inbound TCP server ───────────────
 *
 * peerModeName is defined down in the CLI section; forward-declare it so the
 * accept handler can log the mode.
 */
static const char* peerModeName(uint8_t m);

/*
 * The Incoming Ports collection (s.tcp.servers): up to TCP_MAX_SERVERS TCP
 * listeners, each its own net endpoint, together accepting up to
 * TCP_MAX_INBOUND connections. Each accepted connection becomes its own rnsd
 * interface "tcp_in/<addr>#<slot>", framed identically to the outbound peers
 * (HDLC) and carrying its server's mode, community radius + IFAC. Runs on the
 * same tcp task. */

struct server_t {
    char     idstr[12];          /* collection id — stable across reorders */
    bool     enabled;
    uint16_t port;
    uint8_t  mode;
    int      max_conns;          /* per-port connection ceiling */
    uint8_t  community_radius;   /* serve nodes within this many hops; 0 = none */
    uint8_t  ifac_size;
    uint8_t  announce_cap;
    char     ifac_netname[32];
    char     ifac_netkey[64];
    char     nvsKey[16];         /* net endpoint identity: "tcp_srv_<id>" */
};

struct inbound_peer_t {
    bool     used;
    int      net_handle;     /* ITS server handle (TCP byte stream from net) */
    int      rnsd_handle;    /* ITS handle to rnsd (RNS packet stream) */
    uint8_t  mode;
    int      srv;            /* index into s_servers at accept time */
    char     addr[48];       /* client "ip" label */

    /* HDLC inbound assembly — same field names as peer_t so hdlcConsume<>
     * works on both. */
    uint8_t  rx_pkt[RNS_MTU + 8];
    size_t   rx_len;
    bool     rx_in_frame;
    bool     rx_escaping;

    bool       rx_drop_open;
    uint32_t   rx_drops;
    uint32_t   rx_drop_bytes;
    TickType_t rx_drop_since;

    uint64_t bytes_in;
    uint64_t bytes_out;
};

PSRAM_BSS static inbound_peer_t s_inbound[TCP_MAX_INBOUND];

static server_t s_servers[TCP_MAX_SERVERS];
static int      s_serverCount = 0;
/* Every nvsKey ever pushed to net this boot: a removed server's endpoint must
 * be closed by name (tcpPort 0), or net keeps its socket open forever. */
static char     s_regKeys[TCP_MAX_SERVERS * 2][16];
static int      s_regKeyCount = 0;

static uint8_t modeFromStr(const char* m) {
    if      (strcmp(m, "full")         == 0) return RNS_IFACE_MODE_FULL;
    else if (strcmp(m, "gateway")      == 0) return RNS_IFACE_MODE_GATEWAY;
    else if (strcmp(m, "access_point") == 0) return RNS_IFACE_MODE_ACCESS_POINT;
    else if (strcmp(m, "roaming")      == 0) return RNS_IFACE_MODE_ROAMING;
    else if (strcmp(m, "boundary")     == 0) return RNS_IFACE_MODE_BOUNDARY;
    return RNS_IFACE_MODE_ACCESS_POINT;
}

static int inboundActiveCount(void) {
    int n = 0;
    for (auto& ip : s_inbound) if (ip.used) n++;
    return n;
}
static int inboundActiveCountFor(int srv) {
    int n = 0;
    for (auto& ip : s_inbound) if (ip.used && ip.srv == srv) n++;
    return n;
}
static inbound_peer_t* inboundByNetHandle(int h) {
    for (auto& ip : s_inbound) if (ip.used && ip.net_handle == h) return &ip;
    return nullptr;
}
static inbound_peer_t* inboundByRnsdHandle(int h) {
    for (auto& ip : s_inbound) if (ip.used && ip.rnsd_handle == h) return &ip;
    return nullptr;
}

static void loadServerConfig(void) {
    char key[80];
    int n = storageArrayCount("s.tcp.servers");
    if (n > TCP_MAX_SERVERS) n = TCP_MAX_SERVERS;
    s_serverCount = n;
    for (int i = 0; i < n; i++) {
        server_t& sv = s_servers[i];
        sv = server_t{};
        char idbuf[7] = "";
        snprintf(key, sizeof(key), "s.tcp.servers.%d.id", i);
        storageGetStr(key, idbuf, sizeof(idbuf), "");
        safeStrncpy(sv.idstr, idbuf, sizeof(sv.idstr));
        snprintf(sv.nvsKey, sizeof(sv.nvsKey), "tcp_srv_%s", idbuf);
        snprintf(key, sizeof(key), "s.tcp.servers.%d.enable", i);
        sv.enabled = storageGetInt(key, 1) != 0;
        snprintf(key, sizeof(key), "s.tcp.servers.%d.port", i);
        sv.port = (uint16_t)storageGetInt(key, 4965);
        char mode[24] = "access_point";
        snprintf(key, sizeof(key), "s.tcp.servers.%d.mode", i);
        storageGetStr(key, mode, sizeof(mode), "access_point");
        sv.mode = modeFromStr(mode);
        snprintf(key, sizeof(key), "s.tcp.servers.%d.max_conns", i);
        sv.max_conns = storageGetInt(key, TCP_MAX_INBOUND);
        if (sv.max_conns < 0) sv.max_conns = 0;
        if (sv.max_conns > TCP_MAX_INBOUND) sv.max_conns = TCP_MAX_INBOUND;
        /* Radius 0 by default — same reasoning as an outbound peer: whoever
         * dials in is on the cheap side of the node, and their announces are
         * someone else's traffic. */
        snprintf(key, sizeof(key), "s.tcp.servers.%d.community_radius", i);
        sv.community_radius = (uint8_t)storageGetInt(key, 0);
        snprintf(key, sizeof(key), "s.tcp.servers.%d.ifac_netname", i);
        storageGetStr(key, sv.ifac_netname, sizeof(sv.ifac_netname), "");
        snprintf(key, sizeof(key), "s.tcp.servers.%d.ifac_netkey", i);
        storageGetStr(key, sv.ifac_netkey, sizeof(sv.ifac_netkey), "");
        snprintf(key, sizeof(key), "s.tcp.servers.%d.ifac_size", i);
        sv.ifac_size = (uint8_t)storageGetInt(key, 0);
        snprintf(key, sizeof(key), "s.tcp.servers.%d.announce_cap", i);
        sv.announce_cap = (uint8_t)storageGetInt(key, RNS_IFACE_ANNOUNCE_CAP_DEFAULT);
    }
}

static void inboundTeardown(inbound_peer_t& ip, const char* reason) {
    if (ip.rnsd_handle >= 0) { itsDisconnect(ip.rnsd_handle); ip.rnsd_handle = -1; }
    if (ip.net_handle  >= 0) { itsDisconnect(ip.net_handle);  ip.net_handle  = -1; }
    flushRnsdDrops(&ip, /*force=*/true);
    ip.rx_drop_open = false;
    if (ip.used) info("tcp inbound: %s closed (%s)", ip.addr, reason);
    ip.used = false;
}

static void onInboundRnsdRecv(int handle, size_t /*bytesAvail*/) {
    PSRAM_BSS static uint8_t pkt[RNS_MTU + 16];
    size_t n = itsRecv(handle, pkt, sizeof(pkt), 0);
    if (n == 0) return;
    inbound_peer_t* ip = inboundByRnsdHandle(handle);
    if (!ip || ip->net_handle < 0) return;
    if (!hdlcSend(ip->net_handle, pkt, n)) { warn("tcp inbound: hdlc send failed"); return; }
    ip->bytes_out += n;
}
static void onInboundRnsdDisconnect(int ref) {
    if (ref < 0 || ref >= TCP_MAX_INBOUND) return;
    inbound_peer_t& ip = s_inbound[ref];
    if (!ip.used) return;
    ip.rnsd_handle = -1;
    inboundTeardown(ip, "rnsd closed");
}
static void onInboundRecv(int handle, size_t /*bytesAvail*/) {
    PSRAM_BSS static uint8_t buf[1024];
    size_t n = itsRecv(handle, buf, sizeof(buf), 0);
    if (n == 0) return;
    inbound_peer_t* ip = inboundByNetHandle(handle);
    if (!ip) return;
    ip->bytes_in += n;
    hdlcConsume(ip, buf, n);
}
static void onInboundDisconnectNet(int ref) {
    if (ref < 0 || ref >= TCP_MAX_INBOUND) return;
    inbound_peer_t& ip = s_inbound[ref];
    if (!ip.used) return;
    ip.net_handle = -1;          /* net already closed this side */
    inboundTeardown(ip, "net closed");
}

static int onInboundConnect(int srv, int handle, const void* data, size_t len) {
    if (srv >= s_serverCount) return -1;       /* server removed — refuse */
    server_t& sv = s_servers[srv];
    if (!sv.enabled) return -1;                /* soft-disabled — refuse */
    if (inboundActiveCountFor(srv) >= sv.max_conns) {
        warn("tcp inbound: port %u at capacity (%d), rejecting", (unsigned)sv.port, sv.max_conns);
        return -1;
    }
    /* The slot index IS the serverRef returned to net AND the ref we hand rnsd,
     * so both disconnect callbacks resolve the same slot. */
    int slot = -1;
    for (int i = 0; i < TCP_MAX_INBOUND; i++) if (!s_inbound[i].used) { slot = i; break; }
    if (slot < 0) return -1;
    inbound_peer_t& ip = s_inbound[slot];
    ip = inbound_peer_t{};
    ip.used = true;
    ip.net_handle = handle;
    ip.rnsd_handle = -1;
    ip.mode = sv.mode;
    ip.srv  = srv;

    const char* ipstr = "?";
    if (len >= sizeof(net_connect_t)) {
        auto* cd = (const net_connect_t*)data;
        ipstr = ipaddr_ntoa(&cd->clientAddr);
    }
    safeStrncpy(ip.addr, ipstr, sizeof(ip.addr));

    rnsd_iface_t reg = {};
    snprintf(reg.name, sizeof(reg.name), "tcp_in/%s#%d", ipstr, slot);
    reg.mtu     = RNS_MTU;
    reg.bitrate = 1000000;  /* 1 Mbps — feeds RNS first-hop link timeout */
    reg.mode    = sv.mode;
    reg.in = reg.out = 1;
    reg.fwd = (sv.mode == RNS_IFACE_MODE_GATEWAY || sv.mode == RNS_IFACE_MODE_FULL) ? 1 : 0;
    reg.rpt = 0;
    reg.ifac_size = sv.ifac_size;
    reg.announce_cap = sv.announce_cap;
    reg.point_to_point = 1;   /* one peer per accepted TCP connection */
    reg.community_radius = sv.community_radius;
    safeStrncpy(reg.ifac_netname, sv.ifac_netname, sizeof(reg.ifac_netname));
    safeStrncpy(reg.ifac_netkey,  sv.ifac_netkey,  sizeof(reg.ifac_netkey));

    ip.rnsd_handle = itsConnect("rnsd", RNSD_PORT_IFACE, &reg, sizeof(reg),
                                pdMS_TO_TICKS(500), slot, onInboundRnsdRecv, onInboundRnsdDisconnect);
    if (ip.rnsd_handle < 0) {
        warn("tcp inbound: rnsd register failed for %s", ip.addr);
        ip.used = false;
        return -1;                              /* net closes the socket */
    }
    info("tcp inbound: port %u accepted %s as iface %s (mode=%s)",
         (unsigned)sv.port, ip.addr, reg.name, peerModeName(sv.mode));
    return slot;
}

/* itsServerOnConnect carries no context, so each server slot's ITS port gets
 * its own trampoline naming the slot. */
static int onInboundConnect0(int h, const void* d, size_t l) { return onInboundConnect(0, h, d, l); }
static int onInboundConnect1(int h, const void* d, size_t l) { return onInboundConnect(1, h, d, l); }
static int onInboundConnect2(int h, const void* d, size_t l) { return onInboundConnect(2, h, d, l); }
static int onInboundConnect3(int h, const void* d, size_t l) { return onInboundConnect(3, h, d, l); }
static its_connect_cb_t const s_inboundConnectCbs[TCP_MAX_SERVERS] =
    { onInboundConnect0, onInboundConnect1, onInboundConnect2, onInboundConnect3 };

static void regKeyRemember(const char* k) {
    for (int i = 0; i < s_regKeyCount; i++)
        if (strcmp(s_regKeys[i], k) == 0) return;
    if (s_regKeyCount < (int)(sizeof(s_regKeys) / sizeof(s_regKeys[0])))
        safeStrncpy(s_regKeys[s_regKeyCount++], k, sizeof(s_regKeys[0]));
}

static void serverEndpointPush(const char* nvsKey, int slot, uint16_t port) {
    net_port_msg_t reg = {};
    reg.itsPort    = (uint16_t)(TCP_PORT_INBOUND + slot);
    reg.ownPort    = 1;
    reg.tcpPort    = port;    /* 0 => net closes the socket */
    reg.tcpNoDelay = 1;
    reg.keepAlive  = 1;
    reg.backlog    = 4;
    safeStrncpy(reg.nvsKey, nvsKey, sizeof(reg.nvsKey));
    if (!itsSendAux("net", NET_PORT_REG_PORT, &reg, sizeof(reg), pdMS_TO_TICKS(500)))
        warn("tcp: inbound server net registration failed (%s)", nvsKey);
}

/* Push every server's desired listen state to net, and close the endpoint of
 * any server registered earlier this boot but no longer configured. Re-sending
 * with a new port is how a runtime port change takes effect: net rebinds on
 * its next poll (see epOpenPort). */
static void serversRegister(void) {
    for (int i = 0; i < s_serverCount; i++) {
        server_t& sv = s_servers[i];
        serverEndpointPush(sv.nvsKey, i, sv.enabled ? sv.port : 0);
        regKeyRemember(sv.nvsKey);
    }
    for (int i = 0; i < s_regKeyCount; i++) {
        bool live = false;
        for (int j = 0; j < s_serverCount; j++)
            if (strcmp(s_regKeys[i], s_servers[j].nvsKey) == 0) { live = true; break; }
        if (!live) serverEndpointPush(s_regKeys[i], 0, 0);
    }
}

/* Reconcile server state with config — runs on the tcp task on config change.
 * mode/IFAC are baked at accept time, so a change drops live inbound peers to
 * force them to re-register with the new settings on reconnect. */
static void reconcileServer(void) {
    server_t old[TCP_MAX_SERVERS];
    int oldCount = s_serverCount;
    memcpy(old, s_servers, sizeof(old));

    loadServerConfig();

    /* Registration values (mode, radius, IFAC, cap) are baked at accept time,
     * so any change in the array drops every live inbound connection and lets
     * them come back with the new settings. Coarse and correct: inbound peers
     * redial on their own. */
    bool changed = oldCount != s_serverCount ||
                   memcmp(old, s_servers, sizeof(server_t) * (size_t)s_serverCount) != 0;
    if (changed) {
        serversRegister();
        for (auto& ip : s_inbound) if (ip.used) inboundTeardown(ip, "server settings changed");
    }
}

/* ─────────────── CLI ─────────────── */

static const char* peerStateName(peer_state_t s) {
    switch (s) {
        case PS_IDLE:       return "idle";
        case PS_CONNECTING: return "connecting";
        case PS_UP:         return "up";
        case PS_BACKOFF:    return "backoff";
    }
    return "?";
}

static const char* peerModeName(uint8_t m) {
    switch (m) {
        case RNS_IFACE_MODE_FULL:         return "full";
        case RNS_IFACE_MODE_GATEWAY:      return "gateway";
        case RNS_IFACE_MODE_ACCESS_POINT: return "access_point";
        case RNS_IFACE_MODE_ROAMING:      return "roaming";
        case RNS_IFACE_MODE_BOUNDARY:     return "boundary";
    }
    return "gateway";
}

/* Validate a mode string; returns the canonical name or nullptr if unknown. */
static const char* peerModeCanonical(const char* s) {
    if (strcmp(s, "full")         == 0) return "full";
    if (strcmp(s, "gateway")      == 0) return "gateway";
    if (strcmp(s, "access_point") == 0) return "access_point";
    if (strcmp(s, "roaming")      == 0) return "roaming";
    if (strcmp(s, "boundary")     == 0) return "boundary";
    return nullptr;
}

static void cliTcpStatus(void)
{
    cliPrintf("global: %s%s\n", s_globalEnable ? "enabled" : "disabled",
              (s_globalEnable && !s_upstreamUp) ? " (waiting for upstream)" : "");
    if (s_peers.empty()) {
        cliPrintf("(no outbound peers configured)\n");
    } else {
        cliPrintf("%-3s %-10s %-28s %-13s %-9s %s\n",
                  "#", "state", "host:port", "mode", "per-peer", "rx/tx");
        for (auto& p : s_peers) {
            char hp[80];
            std::snprintf(hp, sizeof(hp), "%s:%u",
                          p.host[0] ? p.host : "(empty)", (unsigned)p.port);
            cliPrintf("%-3d %-10s %-28s %-13s %-9s rx=%llu tx=%llu\n",
                      p.id, peerStateName(p.state), hp, peerModeName(p.mode),
                      p.enabled ? "enabled" : "disabled",
                      (unsigned long long)p.bytes_in,
                      (unsigned long long)p.bytes_out);
        }
    }

    cliPrintf("incoming ports: %d configured\n", s_serverCount);
    for (int i = 0; i < s_serverCount; i++) {
        server_t& sv = s_servers[i];
        cliPrintf("    port %-5u %-13s %-9s active=%d/%d radius=%u\n",
                  (unsigned)sv.port, peerModeName(sv.mode),
                  sv.enabled ? "enabled" : "disabled",
                  inboundActiveCountFor(i), sv.max_conns,
                  (unsigned)sv.community_radius);
    }
    for (auto& ip : s_inbound) {
        if (!ip.used) continue;
        cliPrintf("    in  %-24s %-13s rx=%llu tx=%llu\n",
                  ip.addr, peerModeName(ip.mode),
                  (unsigned long long)ip.bytes_in,
                  (unsigned long long)ip.bytes_out);
    }
}

static void cliTcpPeerAdd(const char* rest)
{
    while (*rest == ' ') rest++;
    if (!*rest) { cliPrintf("usage: tcp peer add <host[:port]> [mode]\n"); return; }
    const char* sp = std::strchr(rest, ' ');
    std::string hp = sp ? std::string(rest, sp - rest) : std::string(rest);
    const char* mode = sp ? sp + 1 : "access_point";
    while (*mode == ' ') mode++;
    if (!*mode) mode = "access_point";

    /* Split host:port. ':' from the right so IPv6-ish forms could be
     * extended later; for now just simple host:port or bare hostname. */
    std::string host;
    int port = 4965;
    auto colon = hp.rfind(':');
    if (colon != std::string::npos) {
        host = hp.substr(0, colon);
        port = std::atoi(hp.c_str() + colon + 1);
        if (port <= 0 || port > 65535) {
            cliPrintf("tcp peer add: bad port in \"%s\"\n", hp.c_str());
            return;
        }
    } else {
        host = hp;
    }
    if (host.empty()) {
        cliPrintf("tcp peer add: bad host in \"%s\"\n", hp.c_str());
        return;
    }

    /* Find the lowest free slot. */
    int slot = -1;
    char k[80];
    for (int i = 0; i < TCP_MAX_PEERS; i++) {
        std::snprintf(k, sizeof(k), "s.tcp.peers.%d.host", i);
        if (!storageExists(k)) { slot = i; break; }
    }
    if (slot < 0) {
        cliPrintf("tcp peer add: no free slot (max %d)\n", TCP_MAX_PEERS);
        return;
    }

    /* Keep s.tcp.peers a JSON array (not a numeric-keyed object): the first peer
     * is written as a one-element array via setTree (nothing to preserve);
     * later peers merge-append as array element `slot`. The web reads an array. */
    if (slot == 0) {
        cJSON* arr = cJSON_CreateArray();
        cJSON* o = cJSON_CreateObject();
        cJSON_AddNumberToObject(o, "enable", 1);
        cJSON_AddStringToObject(o, "host", host.c_str());
        cJSON_AddNumberToObject(o, "port", port);
        cJSON_AddStringToObject(o, "mode", mode);
        cJSON_AddItemToArray(arr, o);
        storageSetTree("s.tcp.peers", arr);
    } else {
        storageBegin();
        std::snprintf(k, sizeof(k), "s.tcp.peers.%d.host",   slot); storageSet(k, host.c_str());
        std::snprintf(k, sizeof(k), "s.tcp.peers.%d.port",   slot); storageSet(k, port);
        std::snprintf(k, sizeof(k), "s.tcp.peers.%d.mode",   slot); storageSet(k, mode);
        std::snprintf(k, sizeof(k), "s.tcp.peers.%d.enable", slot); storageSet(k, 1);
        storageEnd();
    }

    cliPrintf("tcp: added peer at slot %d: %s:%d (mode=%s)\n",
              slot, host.c_str(), port, mode);
}

static void cliTcpPeer(const char* rest)
{
    while (*rest == ' ') rest++;
    if (!*rest) {
        cliPrintf("usage: tcp peer add|rm|enable|disable …\n");
        return;
    }
    const char* sp = std::strchr(rest, ' ');
    std::string sub = sp ? std::string(rest, sp - rest) : std::string(rest);
    const char* rest2 = sp ? sp + 1 : "";

    if (sub == "add") { cliTcpPeerAdd(rest2); return; }

    if (sub == "mode") {
        while (*rest2 == ' ') rest2++;
        const char* sp2 = std::strchr(rest2, ' ');
        if (!sp2) {
            cliPrintf("usage: tcp peer mode <slot> <full|gateway|access_point|roaming|boundary>\n");
            return;
        }
        std::string slotStr(rest2, sp2 - rest2);
        const char* modeStr = sp2 + 1;
        while (*modeStr == ' ') modeStr++;
        char* end = nullptr;
        long n = std::strtol(slotStr.c_str(), &end, 10);
        if (!end || *end != '\0' || n < 0 || n >= TCP_MAX_PEERS) {
            cliPrintf("tcp peer mode: bad slot \"%s\"\n", slotStr.c_str());
            return;
        }
        const char* canon = peerModeCanonical(modeStr);
        if (!canon) {
            cliPrintf("tcp peer mode: bad mode \"%s\" (full|gateway|access_point|roaming|boundary)\n", modeStr);
            return;
        }
        char k[80];
        std::snprintf(k, sizeof(k), "s.tcp.peers.%ld.host", n);
        if (!storageExists(k)) {
            cliPrintf("tcp peer mode: no peer at slot %ld\n", n);
            return;
        }
        std::snprintf(k, sizeof(k), "s.tcp.peers.%ld.mode", n);
        storageSet(k, canon);
        cliPrintf("tcp: peer %ld mode set to %s\n", n, canon);
        return;
    }

    while (*rest2 == ' ') rest2++;
    if (!*rest2) {
        cliPrintf("usage: tcp peer %s <slot>\n", sub.c_str());
        return;
    }
    char* end = nullptr;
    long n = std::strtol(rest2, &end, 10);
    if (!end || *end != '\0' || n < 0 || n >= TCP_MAX_PEERS) {
        cliPrintf("tcp peer %s: bad slot \"%s\"\n", sub.c_str(), rest2);
        return;
    }
    char k[80];
    std::snprintf(k, sizeof(k), "s.tcp.peers.%ld.host", n);
    if (!storageExists(k)) {
        cliPrintf("tcp peer %s: no peer at slot %ld\n", sub.c_str(), n);
        return;
    }

    if (sub == "rm") {
        /* storageUnset (not storageDeleteTree) so the array element is removed
         * AND the rest shift down, and the s.tcp.peers subscription fires. */
        storageBegin();
        std::snprintf(k, sizeof(k), "s.tcp.peers.%ld", n); storageUnset(k);
        storageEnd();
        cliPrintf("tcp: removed peer slot %ld\n", n);
        return;
    }
    if (sub == "enable") {
        std::snprintf(k, sizeof(k), "s.tcp.peers.%ld.enable", n);
        storageSet(k, 1);
        cliPrintf("tcp: peer %ld enabled\n", n);
        return;
    }
    if (sub == "disable") {
        std::snprintf(k, sizeof(k), "s.tcp.peers.%ld.enable", n);
        storageSet(k, 0);
        cliPrintf("tcp: peer %ld disabled\n", n);
        return;
    }
    cliPrintf("unknown peer subcommand `%s`. try `tcp -h`.\n", sub.c_str());
}

static void cliTcp(const char* args)
{
    if (!args) args = "";
    while (*args == ' ') args++;

    if (!*args) { cliTcpStatus(); return; }

    if (std::strcmp(args, "help") == 0) { cliPrintf("%-*s TCP interface status; peers; start/stop\n", CLI_HELP_COL, "tcp [...]"); return; }
    if (cliWantsHelp(args)) {
        cliPrintf("tcp                              list peers + status\n");
        cliPrintf("tcp start | stop | restart       global gate (s.tcp.enable)\n");
        cliPrintf("tcp server                       incoming-port status (s.tcp.servers)\n");
        cliPrintf("tcp connect <slot>               force-connect peer (clear backoff)\n");
        cliPrintf("tcp disconnect <slot>            kick peer's connection\n");
        cliPrintf("tcp peer add <host[:port]> [mode] add a peer (port=4965, mode=access_point)\n");
        cliPrintf("tcp peer rm <slot>               remove peer slot\n");
        cliPrintf("tcp peer enable <slot>           persistently enable\n");
        cliPrintf("tcp peer disable <slot>          persistently disable\n");
        cliPrintf("tcp peer mode <slot> <mode>      full|gateway|access_point|roaming|boundary\n");
        return;
    }

    const char* sp = std::strchr(args, ' ');
    std::string verb = sp ? std::string(args, sp - args) : std::string(args);
    const char* rest = sp ? sp + 1 : "";

    if (verb == "start")      { storageSet("s.tcp.enable", 1); cliPrintf("tcp: started\n"); return; }
    if (verb == "stop")       { storageSet("s.tcp.enable", 0); cliPrintf("tcp: stopped\n"); return; }
    if (verb == "restart")    { storageSet("tcp.cmd.restart",    1); cliPrintf("tcp: restart requested\n"); return; }

    if (verb == "connect" || verb == "disconnect") {
        while (*rest == ' ') rest++;
        if (!*rest) { cliPrintf("usage: tcp %s <slot>\n", verb.c_str()); return; }
        char* end = nullptr;
        long n = std::strtol(rest, &end, 10);
        if (!end || *end != '\0' || n < 0 || n >= TCP_MAX_PEERS) {
            cliPrintf("tcp %s: bad slot \"%s\"\n", verb.c_str(), rest);
            return;
        }
        const char* sentinel = (verb == "connect") ? "tcp.cmd.connect"
                                                   : "tcp.cmd.disconnect";
        storageSet(sentinel, (int)n);
        cliPrintf("tcp: %s %ld requested\n", verb.c_str(), n);
        return;
    }

    if (verb == "peer") { cliTcpPeer(rest); return; }

    if (verb == "server") { cliTcpStatus(); return; }

    cliPrintf("unknown subcommand `%s`. try `tcp -h`.\n", verb.c_str());
}

/* ─────────────── net events ───────────────
 *
 * Bound to NET_EV_UPSTREAM_UP/DOWN (not NET_EV_UP/DOWN): we only dial when
 * there's a real STA upstream to reach peers over. AP-only mode fires UP but
 * not UPSTREAM_UP, so peers stay idle there. Run on the net task (DOWN edge)
 * or, for the level-replayed UP edge, possibly on the tcp task at registration
 * time. Both only flip volatile flags + notify — the reconcile (tear-down /
 * redial) happens on the tcp task in servicePeers, which alone owns s_peers. */

static void onUpstreamUp(const char*)
{
    if (s_upstreamUp) return;
    s_upstreamUp = true;
    s_netEdge    = true;       /* clear backoff so peers dial without delay */
    if (s_task) xTaskNotifyGive(s_task);
}

static void onUpstreamDown(const char*)
{
    if (!s_upstreamUp) return;
    s_upstreamUp = false;
    if (s_task) xTaskNotifyGive(s_task);   /* servicePeers tears peers down */
}

/* ─────────────── Task ─────────────── */

static void tcpTaskMain(void*)
{
    info("[%s] task up", TAG);

    /* No boot barrier here anymore: the RNS orchestrator only calls tcpStart()
     * (which spawns this task) after rnsd is up and past its boot window, so the
     * network we ride on is already settled by the time we run. */
    itsClientInit(TCP_MAX_PEERS * 2 + TCP_MAX_INBOUND);

    /* Inbound TCP server: one ITS server port; net connects to it per accepted
     * client. Open the port + handlers regardless of enable so config can flip
     * it on later; the listen socket is registered with net only when enabled. */
    itsServerInit();
    /* One ITS server port per Incoming Ports slot: the port is how a
     * connection names which listener accepted it (itsServerOnConnect carries
     * no context). Recv/disconnect resolve the peer by handle, so those
     * handlers are shared. */
    for (int i = 0; i < TCP_MAX_SERVERS; i++) {
        uint16_t port = (uint16_t)(TCP_PORT_INBOUND + i);
        itsServerPortOpen(port, /*packetBased=*/false, TCP_MAX_INBOUND, 4096, 4096);
        itsServerOnConnect(port,    s_inboundConnectCbs[i]);
        itsServerOnRecv(port,       onInboundRecv);
        itsServerOnDisconnect(port, onInboundDisconnectNet);
    }
    loadServerConfig();
    serversRegister();

    /* Cache the global gate. Default 1 — no key in storage means "on";
     * user must explicitly set s.tcp.enable=0 to stop. */
    s_globalEnable = storageGetInt("s.tcp.enable", 1) != 0;

    /* Seed upstream state, then subscribe. NET_EV_UPSTREAM_UP is level-replayed,
     * so if we're already STA-connected onUpstreamUp fires immediately (and
     * no-ops since we seeded s_upstreamUp). */
    s_upstreamUp = netIsStaConnected();
    /* Register net callbacks once for the process — net's registry is append-only
     * (no unregister), so re-registering per rns start would pile up duplicates.
     * The callbacks guard s_task, so staying live across a stop is harmless. */
    static bool s_netCbsRegistered = false;
    if (!s_netCbsRegistered) {
        s_netCbsRegistered = true;
        netRegister(NET_EV_UPSTREAM_UP,   onUpstreamUp);
        netRegister(NET_EV_UPSTREAM_DOWN, onUpstreamDown);
    }

    storageSubscribeChanges("s.tcp.peers",        onCfgChange);
    storageSubscribeChanges("s.tcp.servers",      onCfgChange);  /* incoming-ports cfg */
    storageSubscribeChanges("s.tcp.enable",       onGlobalEnableChange);
    storageSubscribeChanges("tcp.cmd.connect",    onCmdConnect);
    storageSubscribeChanges("tcp.cmd.disconnect", onCmdDisconnect);
    storageSubscribeChanges("tcp.cmd.restart",    onCmdRestart);
    storageSubscribeChanges("tcp.cmd.del",        onCmdDel);

    /* The settings collection. The UI never writes s.tcp.peers — it writes
     * these, and this file is the array's only writer, which is what puts
     * validation in one place and lets a rejection come back as a sentence. */
    peerEnsureIds();
    storageSubscribeChanges("tcp.peer.add",     onPeerAdd);
    storageSubscribeChanges("tcp.peer.set",     onPeerSet);
    storageSubscribeChanges("tcp.peer.remove",  onPeerRemove);
    storageSubscribeChanges("tcp.peer.order",   onPeerOrder);
    storageSubscribeChanges("tcp.peer.connect", onPeerConnect);
    storageSubscribeChanges("tcp.server.add",    onSrvAdd);
    storageSubscribeChanges("tcp.server.set",    onSrvSet);
    storageSubscribeChanges("tcp.server.remove", onSrvRemove);
    storageSubscribeChanges("tcp.server.order",  onSrvOrder);

    /* Clock was already resolved by rnsd before it declared ready (its own
     * waitForTime + boot window ran first), so we don't wait again here.
     * Config subs above queue and dispatch on the first itsPoll below. */

  for (;;) {   /* Park, don't delete: this task lives across rns stop/start, so its
                * ITS client slot, server port + storage subs are reused, not leaked. */
    /* Re-bring-up (first entry + every resume): a dirty pass runs reloadPeers()
     * — which rebuilds the peer vector fresh so enabled peers redial — and
     * reconcileServer(), which re-registers the inbound listen endpoint with net
     * (teardown sent tcpPort=0, so net reopens the socket here). */
    s_configDirty = true;
    while (!s_stop) {
        if (s_configDirty) { s_configDirty = false; reloadPeers(); reconcileServer(); }

        if (s_netEdge) {
            s_netEdge = false;
            /* WiFi just returned — drop backoff accrued while down so
             * enabled peers redial on this tick instead of waiting it out. */
            TickType_t now = xTaskGetTickCount();
            for (auto& p : s_peers) {
                if (p.state == PS_IDLE || p.state == PS_BACKOFF) {
                    p.cur_backoff_s     = 0;
                    p.next_attempt_tick = now;
                }
            }
        }

        servicePeers();

        /* Close out any drop window whose frames stopped arriving. nextDeadline
         * caps at 1 s while a peer is dialable, so the tail lands within a
         * second of the window expiring. */
        for (auto& p : s_peers) flushRnsdDrops(&p);
        for (auto& ip : s_inbound) if (ip.used) flushRnsdDrops(&ip);

        /* Publish peer stats at ~1 Hz. State transitions already publish
         * immediately from attemptConnect/disconnectPeer; this periodic pass
         * only refreshes the tx/rx byte counters. Gating it is essential: a
         * busy peer makes itsPoll return early on every RX, so publishing
         * unconditionally re-committed the ever-changing byte counts to
         * storage on every loop iteration — that churned cJSON nonstop on
         * CPU0 (tripping the task WDT) and fired change-subscriptions faster
         * than subscribers could drain them (notify drops). The 1 s cap in
         * nextDeadline guarantees this still runs even when fully idle. */
        TickType_t pubNow = xTaskGetTickCount();
        if ((int32_t)(pubNow - s_nextPublishTick) >= 0) {
            /* Bracket the whole pass: every peer's 5 stat keys commit as one
             * storage op instead of 5×N sync round-trips, so the 1 Hz publish
             * can't jam the storage op port during an inbound-message burst. */
            storageBegin();
            for (auto& p : s_peers) publishPeerState(p);
            storageEnd();
            s_nextPublishTick = pubNow + pdMS_TO_TICKS(1000);
        }

        itsPoll(nextDeadline());
    }

    /* rns stop: close every open socket we hold so a restart doesn't leak fds —
     * each outbound peer's net + rnsd handles, each live inbound conn's handles,
     * and the inbound listen socket on net (tcpPort 0 => net closes it). Release
     * the peer vector's heap; the re-bring-up rebuilds it from config on resume.
     * s_inbound is PSRAM_BSS — teardown its conns but never free the array. The
     * task parks rather than deleting, so its ITS ports + storage subs stay live
     * and are reused on the next start; rnsd deregisters our ifaces as the
     * handles drop (dropping the rnsd conns frees rnsd's iface slots). */
    for (auto& p : s_peers) {
        if (p.rnsd_handle >= 0) { itsDisconnect(p.rnsd_handle); p.rnsd_handle = -1; }
        if (p.net_handle  >= 0) { itsDisconnect(p.net_handle);  p.net_handle  = -1; }
    }
    for (auto& ip : s_inbound) if (ip.used) inboundTeardown(ip, "tcp stopping");
    for (int i = 0; i < s_regKeyCount; i++)
        serverEndpointPush(s_regKeys[i], 0, 0);   /* 0 => net closes the socket */
    s_regKeyCount = 0;
    std::vector<peer_t>().swap(s_peers);

    s_parked = true;
    info("[%s] stopped", TAG);
    while (s_stop) itsPoll(portMAX_DELAY);   /* park on the inbox until tcpStart un-parks */
    s_parked = false;
  }
}

/* ── RNS lifecycle hooks (registered with the orchestrator; see rnsServiceRegister) ── */
static void tcpStart(void) {
    s_stop = false;
    if (!s_task)
        s_task = spawnTask(tcpTaskMain, TAG, 6144, nullptr, 1, 0, STACK_PSRAM);
    else
        xTaskNotifyGive(s_task);   /* un-park the resident task */
}

static void tcpStop(void) {
    if (!s_task || s_stop) return;
    s_stop = true;
    xTaskNotifyGive(s_task);   /* break the work loop; the task parks, not deleted */
    for (int i = 0; i < 300 && !s_parked; i++) delay(10);   /* await park */
    if (!s_parked) warn("[%s] stop timed out", TAG);
}

void TcpService::onInit()
{
    if (storageGetInt("s.tcp.version", 0) < TCP_VERSION) {
        storageBegin();
        /* Seed both lists as ARRAYS. Without it the first `s.tcp.peers.0.*`
         * write lands as an object keyed "0" — a patch tree is nested objects and
         * there is no array underneath to merge element-wise into. Every reader
         * here counts either shape, but the tree goes to the browser verbatim. */
        storageDefaultTree("s.tcp", "{\"peers\":[],\"servers\":[]}");
        storageSet("s.tcp.version", TCP_VERSION);
        storageEnd();
    }

    cliRegisterCmd("tcp", cliTcp);

    /* Register with the RNS orchestrator instead of self-spawning: rnsStart()
     * calls tcpStart() (which spawns tcpTaskMain) once rnsd is up and past its
     * boot window, and rnsStop() calls tcpStop(). Core 0 alongside net + rnsd,
     * prio 1, PSRAM stack. */
    rnsServiceRegister(TAG, tcpStart, tcpStop, RNS_PHASE_IFACE);
}
