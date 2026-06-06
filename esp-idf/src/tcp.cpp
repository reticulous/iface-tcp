/**
 * tcp — TCP transport task.
 *
 * Phase 1 scope: outbound side only. Watches s.tcp.peers[] and for each
 * enabled peer:
 *   1. Dials via net's NET_PORT_TCP_DIAL with "host:port" payload.
 *   2. On success, registers with rnsd via RNSD_PORT_TRANSPORT as
 *      iface "tcp/<id>".
 *   3. Shuttles bytes both ways with HDLC framing on the net handle
 *      (FLAG=0x7E, ESC=0x7D, ESC_MASK=0x20).
 *   4. Reconnect with exponential backoff per s.tcp.peers[i].retry_*.
 *
 * Inbound (TCP server) and the s.tcp.server_* config keys land in
 * Phase 5 (config-disabled by default per the plan §16).
 */
#include "tcp.h"
#include "spangap.h"
#include "net.h"          /* netRegister, netIsUp, NET_EV_*, NET_PORT_TCP_DIAL */
#include "ports.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

static const char* TAG = "tcp";

#define TCP_VERSION    1
#define TCP_MAX_PEERS  16
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

    uint64_t bytes_in;
    uint64_t bytes_out;
};

static std::vector<peer_t> s_peers;
static int s_next_runtime_id = 1;
static TaskHandle_t s_task = nullptr;
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

/* Net-up gate. A peer can only dial while WiFi is actually up. Dialing
 * with net down is pointless — net rejects the dial (`!netIsStaConnected`)
 * — and the doomed attempt still spends a connect/teardown cycle, logs,
 * and cycles the rnsd iface register/deregister path. Worse, it keeps the
 * tcp task waking on backoff deadlines that can never succeed, defeating
 * light sleep when off-WiFi. We track net via NET_EV_UP/DOWN and only
 * attempt connects while up; on net-down peers are torn down once here,
 * on net-up backoff is cleared so they redial promptly. Written from the
 * net task (edge events) and read on the tcp task — volatile, single-word. */
static volatile bool s_netUp   = false;
static volatile bool s_netEdge = false;   /* net just came up — clear backoff, redial now */

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

/* Decode bytes from net into the peer's pkt buffer. On a complete frame,
 * forward to rnsd as one ITS packet and reset assembly state. */
static void hdlcConsume(peer_t* p, const uint8_t* in, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        uint8_t b = in[i];
        if (b == HDLC_FLAG) {
            if (p->rx_in_frame && p->rx_len > 0) {
                /* End of frame — emit to rnsd. */
                if (p->rnsd_handle >= 0) {
                    size_t s = itsSend(p->rnsd_handle, p->rx_pkt, p->rx_len, pdMS_TO_TICKS(100));
                    if (s == 0) warn("rnsd ITS send dropped (%zu B)", p->rx_len);
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
    char mode[24] = "gateway";
    storageGetStr(key, mode, sizeof(mode), "gateway");
    if      (strcmp(mode, "full")         == 0) p.mode = RNS_IFACE_MODE_FULL;
    else if (strcmp(mode, "gateway")      == 0) p.mode = RNS_IFACE_MODE_GATEWAY;
    else if (strcmp(mode, "access_point") == 0) p.mode = RNS_IFACE_MODE_ACCESS_POINT;
    else if (strcmp(mode, "roaming")      == 0) p.mode = RNS_IFACE_MODE_ROAMING;
    else if (strcmp(mode, "boundary")     == 0) p.mode = RNS_IFACE_MODE_BOUNDARY;
    else                                        p.mode = RNS_IFACE_MODE_GATEWAY;
    snprintf(key, sizeof(key), "s.tcp.peers.%d.retry_min", id);
    p.retry_min_s = (uint32_t)storageGetInt(key, 2);
    snprintf(key, sizeof(key), "s.tcp.peers.%d.retry_max", id);
    p.retry_max_s = (uint32_t)storageGetInt(key, 300);
    if (p.retry_min_s == 0) p.retry_min_s = 2;
    if (p.retry_max_s < p.retry_min_s) p.retry_max_s = p.retry_min_s;
}

static void publishPeerState(peer_t& p)
{
    char key[64];
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
    snprintf(key, sizeof(key), "tcp.peers.%d.stats.tx_bytes", p.id); storageSet(key, (int)(p.bytes_out & 0x7fffffff));
    snprintf(key, sizeof(key), "tcp.peers.%d.stats.rx_bytes", p.id); storageSet(key, (int)(p.bytes_in  & 0x7fffffff));
}

/* ─────────────── connection lifecycle ─────────────── */

static void disconnectPeer(peer_t& p, const char* reason)
{
    if (p.rnsd_handle >= 0) { itsDisconnect(p.rnsd_handle); p.rnsd_handle = -1; }
    if (p.net_handle  >= 0) { itsDisconnect(p.net_handle);  p.net_handle  = -1; }
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
    static uint8_t buf[1024];
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
    static uint8_t pkt[RNS_MTU + 16];
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
    if (!p.enabled || !s_netUp || p.host[0] == '\0' || p.port == 0) return;

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

    rnsd_transport_t reg = {};
    snprintf(reg.name, sizeof(reg.name), "tcp/%d", p.id);
    reg.mtu     = RNS_MTU;
    reg.bitrate = 0;        /* TCP is variable */
    reg.mode    = p.mode;
    reg.in = reg.out = 1;
    reg.fwd = (p.mode == RNS_IFACE_MODE_GATEWAY || p.mode == RNS_IFACE_MODE_FULL) ? 1 : 0;
    reg.rpt = 0;

    p.rnsd_handle = itsConnect("rnsd", RNSD_PORT_TRANSPORT, &reg, sizeof(reg),
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
    /* Nothing dials while WiFi is down OR the global gate is off; sleep until
     * a net-up / config / enable / cmd notify wakes us (no 1 Hz stats churn
     * either — peer state is static, so the chip can stay in light sleep).
     * This MUST match servicePeers' dial gate: if we returned a finite (here
     * 0) deadline for an enabled peer that servicePeers won't actually dial —
     * e.g. an idle peer whose attempt tick is already past while the global
     * gate is off — itsPoll(0) would return instantly every loop, spinning
     * the task and starving IDLE0 → task WDT. */
    if (!s_netUp || !s_globalEnable) return portMAX_DELAY;
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
        bool should_be_up = s_globalEnable && peerDialable(p) && s_netUp;
        if (!should_be_up) {
            if (p.state == PS_UP || p.state == PS_CONNECTING)
                disconnectPeer(p, !s_netUp        ? "net down"
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
            loadPeerConfig(p, i);            /* refreshes all fields including id */
            if (wasEnabled && !p.enabled) {
                disconnectPeer(p, "disabled");
                p.state = PS_IDLE;
                p.cur_backoff_s = 0;
            } else if (!wasEnabled && p.enabled) {
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

static void cliTcpStatus(void)
{
    cliPrintf("global: %s%s\n", s_globalEnable ? "enabled" : "disabled",
              (s_globalEnable && !s_netUp) ? " (waiting for net)" : "");
    if (s_peers.empty()) { cliPrintf("(no peers configured)\n"); return; }
    cliPrintf("%-3s %-10s %-32s %-9s %s\n",
              "#", "state", "host:port", "per-peer", "rx/tx");
    for (auto& p : s_peers) {
        char hp[80];
        std::snprintf(hp, sizeof(hp), "%s:%u",
                      p.host[0] ? p.host : "(empty)", (unsigned)p.port);
        cliPrintf("%-3d %-10s %-32s %-9s rx=%llu tx=%llu\n",
                  p.id, peerStateName(p.state), hp,
                  p.enabled ? "enabled" : "disabled",
                  (unsigned long long)p.bytes_in,
                  (unsigned long long)p.bytes_out);
    }
}

static void cliTcpPeerAdd(const char* rest)
{
    while (*rest == ' ') rest++;
    if (!*rest) { cliPrintf("usage: tcp peer add <host[:port]> [mode]\n"); return; }
    const char* sp = std::strchr(rest, ' ');
    std::string hp = sp ? std::string(rest, sp - rest) : std::string(rest);
    const char* mode = sp ? sp + 1 : "gateway";
    while (*mode == ' ') mode++;
    if (!*mode) mode = "gateway";

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

    /* Write atomically; the s.tcp.peers subscription wakes the tcp
     * task once at storageEnd(). */
    storageBegin();
    std::snprintf(k, sizeof(k), "s.tcp.peers.%d.host",   slot); storageSet(k, host.c_str());
    std::snprintf(k, sizeof(k), "s.tcp.peers.%d.port",   slot); storageSet(k, port);
    std::snprintf(k, sizeof(k), "s.tcp.peers.%d.mode",   slot); storageSet(k, mode);
    std::snprintf(k, sizeof(k), "s.tcp.peers.%d.enable", slot); storageSet(k, 1);
    storageEnd();

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
        std::snprintf(k, sizeof(k), "s.tcp.peers.%ld", n);
        storageDeleteTree(k);
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

    if (std::strcmp(args, "help") == 0) { cliPrintf("%-*s TCP transport status; peers; start/stop\n", CLI_HELP_COL, "tcp [...]"); return; }
    if (cliWantsHelp(args)) {
        cliPrintf("tcp                              list peers + status\n");
        cliPrintf("tcp start | stop | restart       global gate (s.tcp.enable)\n");
        cliPrintf("tcp connect <slot>               force-connect peer (clear backoff)\n");
        cliPrintf("tcp disconnect <slot>            kick peer's connection\n");
        cliPrintf("tcp peer add <host[:port]> [mode] add a peer (port=4965, mode=gateway)\n");
        cliPrintf("tcp peer rm <slot>               remove peer slot\n");
        cliPrintf("tcp peer enable <slot>           persistently enable\n");
        cliPrintf("tcp peer disable <slot>          persistently disable\n");
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

    cliPrintf("unknown subcommand `%s`. try `tcp -h`.\n", verb.c_str());
}

/* ─────────────── net events ───────────────
 *
 * Run on the net task (DOWN edge) or, for the level-replayed UP edge,
 * possibly on the tcp task at registration time. Both only flip volatile
 * flags + notify — the reconcile (tear-down / redial) happens on the tcp
 * task in servicePeers, which alone owns s_peers. */

static void onNetUp(const char*)
{
    if (s_netUp) return;
    s_netUp   = true;
    s_netEdge = true;          /* clear backoff so peers dial without delay */
    if (s_task) xTaskNotifyGive(s_task);
}

static void onNetDown(const char*)
{
    if (!s_netUp) return;
    s_netUp = false;
    if (s_task) xTaskNotifyGive(s_task);   /* servicePeers tears peers down */
}

/* ─────────────── Task ─────────────── */

static void tcpTaskMain(void*)
{
    info("[%s] task up", TAG);

    itsClientInit(TCP_MAX_PEERS * 2);

    /* Cache the global gate. Default 1 — no key in storage means "on";
     * user must explicitly set s.tcp.enable=0 to stop. */
    s_globalEnable = storageGetInt("s.tcp.enable", 1) != 0;

    /* Seed net state, then subscribe. NET_EV_UP is level-replayed, so if
     * WiFi is already up onNetUp fires immediately (and no-ops since we
     * seeded s_netUp). */
    s_netUp = netIsUp();
    netRegister(NET_EV_UP,   onNetUp);
    netRegister(NET_EV_DOWN, onNetDown);

    storageSubscribeChanges("s.tcp.peers",        onCfgChange);
    storageSubscribeChanges("s.tcp.enable",       onGlobalEnableChange);
    storageSubscribeChanges("tcp.cmd.connect",    onCmdConnect);
    storageSubscribeChanges("tcp.cmd.disconnect", onCmdDisconnect);
    storageSubscribeChanges("tcp.cmd.restart",    onCmdRestart);

    for (;;) {
        if (s_configDirty) { s_configDirty = false; reloadPeers(); }

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
            for (auto& p : s_peers) publishPeerState(p);
            s_nextPublishTick = pubNow + pdMS_TO_TICKS(1000);
        }

        itsPoll(nextDeadline());
    }
}

void tcpInit(void)
{
    if (storageGetInt("s.tcp.version", 0) < TCP_VERSION) {
        storageDefault("s.tcp.server_enable", 0);
        storageDefault("s.tcp.server_port", 4965);
        storageSet("s.tcp.version", TCP_VERSION);
    }

    cliRegisterCmd("tcp", cliTcp);

    /* Core 0 alongside net + rnsd, prio 2, PSRAM stack. */
    s_task = spawnTask(tcpTaskMain, TAG, 6144, nullptr, 2, 0, STACK_PSRAM);
}
