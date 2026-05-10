/**
 * tcp — TCP transport task.
 *
 * Phase 1 scope: outbound side only. Watches s.tcp.peers[] and for each
 * enabled peer:
 *   1. Dials via net's NET_PORT_TCP_DIAL with "host:port" payload.
 *   2. On success, registers with rnsd via RNSD_PORT_REGISTER as
 *      iface "tcp/<id>".
 *   3. Shuttles bytes both ways with HDLC framing on the net handle
 *      (FLAG=0x7E, ESC=0x7D, ESC_MASK=0x20).
 *   4. Reconnect with exponential backoff per s.tcp.peers[i].retry_*.
 *
 * Inbound (TCP server) and the s.tcp.server_* config keys land in
 * Phase 5 (config-disabled by default per the plan §16).
 */
#include "tcp.h"
#include "diptych.h"
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

static peer_t* peerByRuntimeId(int rid) {
    for (auto& p : s_peers) if (p.runtime_id == rid) return &p;
    return nullptr;
}

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
    peer_t* p = peerByNetHandle(handle);
    if (!p) return;
    /* Plain static — ITS dispatches recv callbacks only on the task that
     * registered them (tcp task here), so no concurrency. Avoid
     * `thread_local` which on ESP-IDF/libgcc pulls in lazy TLS init that
     * has been seen to corrupt FreeRTOS scheduler state at boot. */
    static uint8_t buf[1024];
    size_t n = itsRecv(handle, buf, sizeof(buf), 0);
    if (n == 0) return;
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
    peer_t* p = peerByRnsdHandle(handle);
    if (!p || p->net_handle < 0) return;
    static uint8_t pkt[RNS_MTU + 16];
    size_t n = itsRecv(handle, pkt, sizeof(pkt), 0);
    if (n == 0) return;
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
    if (!p.enabled || p.host[0] == '\0' || p.port == 0) return;

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

    rnsd_register_t reg = {};
    snprintf(reg.name, sizeof(reg.name), "tcp/%d", p.id);
    reg.mtu     = RNS_MTU;
    reg.bitrate = 0;        /* TCP is variable */
    reg.mode    = p.mode;
    reg.in = reg.out = 1;
    reg.fwd = (p.mode == RNS_IFACE_MODE_GATEWAY || p.mode == RNS_IFACE_MODE_FULL) ? 1 : 0;
    reg.rpt = 0;

    p.rnsd_handle = itsConnect("rnsd", RNSD_PORT_REGISTER, &reg, sizeof(reg),
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
    TickType_t now = xTaskGetTickCount();
    TickType_t soonest = portMAX_DELAY;
    for (auto& p : s_peers) {
        if (!p.enabled) continue;
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
        if (!p.enabled) {
            if (p.state == PS_UP || p.state == PS_CONNECTING) disconnectPeer(p, "disabled");
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

/* ─────────────── CLI ─────────────── */

static void cliRnsdTcp(const char* args)
{
    if (args && strcmp(args, "help") == 0) {
        cliPrintf("  %-*s TCP peer status\n", CLI_HELP_COL, "rnsd tcp");
        return;
    }
    if (s_peers.empty()) { cliPrintf("(no peers configured)\n"); return; }
    for (auto& p : s_peers) {
        const char* st = "idle";
        switch (p.state) {
            case PS_IDLE: st = "idle"; break;
            case PS_CONNECTING: st = "connecting"; break;
            case PS_UP: st = "up"; break;
            case PS_BACKOFF: st = "backoff"; break;
        }
        cliPrintf("[%d] %s %s:%u  state=%s  rx=%llu  tx=%llu\n",
                  p.id, p.enabled ? "enabled" : "disabled",
                  p.host[0] ? p.host : "(empty)", (unsigned)p.port, st,
                  (unsigned long long)p.bytes_in,
                  (unsigned long long)p.bytes_out);
    }
}

/* ─────────────── Task ─────────────── */

static void tcpTaskMain(void*)
{
    info("[%s] task up", TAG);

    itsClientInit(TCP_MAX_PEERS * 2);

    storageSubscribeChanges("s.tcp.peers", onCfgChange);

    for (;;) {
        if (s_configDirty) { s_configDirty = false; reloadPeers(); }
        servicePeers();
        for (auto& p : s_peers) publishPeerState(p);

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

    cliRegisterCmd("rnsd tcp", cliRnsdTcp);

    /* Core 0 alongside net + rnsd, prio 2, PSRAM stack. */
    s_task = spawnTask(tcpTaskMain, TAG, 6144, nullptr, 2, 0, STACK_PSRAM);
}
