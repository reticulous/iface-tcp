# iface-tcp — internals

Maintainer reference for the TCP interface task. The [README](README.md) is the
operator guide; this document is for changing the code without breaking it.

## 1. What this straddle adds

iface-tcp is all-new — it forks nothing. It is a bridge task between two
existing ITS services ([spangap-net](../spangap-net) for TCP, [rns](../rns)'s
`rnsd` for RNS packets). On top of those it provides:

- **One bridge task** (`tcpTaskMain`) that owns all peer and inbound state and
  runs both directions of traffic.
- **HDLC byte-stuffing codec** (`hdlcSend` / `hdlcConsume`) framing RNS packets
  on the TCP byte stream, wire-compatible with upstream Reticulum.
- **Outbound peer table** (`std::vector<peer_t> s_peers`) with a per-peer
  connect/backoff state machine, host:port-stable reconnect across config
  reloads, and the net-upstream dial gate.
- **Inbound TCP server** — one listener (`TCP_PORT_INBOUND` registered with net)
  accepting up to `s.tcp.max_inbound` connections, each a fresh RNS interface.
- **Per-interface IFAC + mode plumbing** — reads the credentials from storage
  and fills `rnsd_iface_t`; the crypto enforcement itself lives in `rnsd` /
  microReticulum.
- **The `tcp` CLI**, the live `tcp.peers.<id>.*` telemetry, and the
  `tcp.cmd.*` command sentinels.
- **The on-device LCD settings pane**
  (`conditional/spangap-lcd/src/tcp_lcd.cpp`, `tcpLcdRegister`) and the browser
  panel (`browser/`).

```
iface-tcp/
├── esp-idf/
│   ├── include/tcp.h          tcpInit() declaration
│   ├── src/tcp.cpp            the tcp task: peers, inbound server, HDLC, CLI
│   └── conditional/spangap-lcd/src/tcp_lcd.cpp   on-device settings pane
└── browser/
    └── src/
        ├── modules/tcp.ts     menu registration (registerTcp)
        └── panels/TcpPanel.vue Settings → Mesh → RNS Interfaces → TCP
```

## 2. The tcp task

One FreeRTOS task, **core 0, prio 2, 6 KB PSRAM stack**, spawned by `tcpInit`.
It is pinned alongside `net` and `rnsd` so the ITS hops between the three stay
on-core. It owns `s_peers`, the `s_inbound[]` table, and the global/server gate
flags; everything that mutates them runs on this task.

**Boot barrier.** The task blocks on `waitForFlag("rns.ready", 120)` before
doing anything — no dialing or listening before `rnsd` and the network it rides
on are up. If `rns.ready` never arrives it `killSelf()`s rather than spin. After
the barrier it also `waitForTime(0)` before the first dial so interfaces don't
register and announce under a 1970 clock.

**Single wait point.** The loop ends in `itsPoll(nextDeadline())`, the only
blocking call. It wakes on an ITS event (recv, disconnect, connect result), a
task notification (`xTaskNotifyGive` from config-change and net-event
callbacks), or the soonest backoff deadline. `nextDeadline` returns
`portMAX_DELAY` when nothing can dial (upstream down or global gate off) so the
chip can stay in light sleep, and otherwise caps the sleep at 1 s so periodic
stats still publish.

**Callbacks run on this task.** ITS recv/disconnect callbacks dispatch only on
the task that registered the handle, and the storage-change subscriptions are
installed from `tcpTaskMain`, so every callback that touches `s_peers` /
`s_inbound` is already on the owning task. The recv buffers are plain
`PSRAM_BSS static`, never `thread_local` — libgcc's lazy TLS init has corrupted
FreeRTOS scheduler state at boot.

## 3. HDLC framing

Identical byte-stuffing to upstream Reticulum's `TCPInterface`:

```
FLAG = 0x7E   ESC = 0x7D   ESC_MASK = 0x20

emit:    FLAG <escaped(packet)> FLAG
escape:  0x7E → 0x7D 0x5E      0x7D → 0x7D 0x5D
decode:  scan for FLAG; inside a frame, ESC means XOR the next byte with 0x20
```

`hdlcSend(netHandle, data, len)` writes one framed packet in a single `itsSend`
(500 ms timeout). The stack buffer is sized `RNS_MTU * 2 + 4` — worst case is
every byte escaped plus the two FLAGs.

`hdlcConsume<P>(p, in, n)` is a streaming decoder templated over `peer_t` and
`inbound_peer_t` (which deliberately share the same `rx_pkt` / `rx_len` /
`rx_in_frame` / `rx_escaping` field names). On each FLAG it emits a non-empty
assembled frame to `p->rnsd_handle` as one ITS packet, then resets. A frame
exceeding `sizeof(rx_pkt)` (`RNS_MTU + 8`) aborts assembly with one `warn` and
resyncs on the next FLAG. The bytes inside a frame are exactly the RNS packet
that would go on any other interface — TCP adds only delimitation.

## 4. ITS topology

Each connection holds two ITS handles on the tcp task:

```
  net  ◄──► net_handle  ─ HDLC ─  rnsd_handle  ◄──►  rnsd
            (TCP bytes)            (RNS packets)
```

- **net → rnsd:** `onNetRecv` drains up to 1 KB from `net_handle` and feeds
  `hdlcConsume`, which emits complete frames to `rnsd_handle`.
- **rnsd → net:** `onRnsdRecv` reads one RNS packet from `rnsd_handle`,
  HDLC-encodes it, and `itsSend`s to `net_handle`.

Both recv callbacks **drain first, then decide**: they always call `itsRecv`
before any early return. Returning before the recv would leave the bytes
buffered and ITS would redispatch the callback forever with nothing consumed —
a busy spin that trips the task WDT (a real hazard, since `rnsd` floods every
interface with the announce stream).

`itsClientInit(TCP_MAX_PEERS * 2 + TCP_MAX_INBOUND)` reserves enough client-side
slots for both directions of every outbound peer plus the inbound rnsd handles.

## 5. Outbound peers

### 5.1 Identity and the runtime_id

A peer's identity is its `host:port`. `peer_t::id` is the current array index
(matches storage and the telemetry key), but it **floats** as peers are
inserted or removed. `runtime_id` is monotonic and stable across reloads, and
it — not `id` — is the ITS connect `ref`. Disconnect callbacks therefore look up
their peer via `peerByRuntimeId(ref)`, which survives index shifts.

### 5.2 State machine

```
PS_IDLE ── attemptConnect ──► PS_CONNECTING ── ok ──► PS_UP
   ▲                              │                     │
   │                              │ dial/register fail  │ disconnect
   │                              ▼                     ▼
   └──────── reload/cmd ──── PS_BACKOFF ◄───────────────┘
                                  │ next_attempt_tick reached
                                  └──► attemptConnect
```

`servicePeers()` runs each tick: it tears down any peer that should not be up
(gate off, disabled, or upstream down) and, for an enabled+dialable peer whose
`next_attempt_tick` has passed, calls `attemptConnect`.

`attemptConnect(p)` dials `net` (`NET_PORT_TCP_DIAL`, 12 s timeout), then opens
the `rnsd` interface (`RNSD_PORT_IFACE`, 500 ms). The `rnsd_iface_t` it sends
names the interface `tcp/<id>`, sets `mtu = RNS_MTU` (500), `bitrate = 1000000`
(1 Mbps — feeds RNS's first-hop link-timeout calc), `in = out = 1`,
`fwd = (mode == gateway || full)`, `rpt = 0`, and the peer's mode + IFAC fields.
On success the peer goes `PS_UP` and `cur_backoff_s` resets to 0.

`disconnectPeer(p, reason)` closes both handles, resets HDLC assembly, and moves
to `PS_BACKOFF` with exponential backoff: `cur_backoff_s` starts at `retry_min`
and doubles up to `retry_max`, scheduling `next_attempt_tick`.

**A peer is only dialable when enabled AND fully addressed.** `peerDialable`
requires a non-empty host and non-zero port. An enabled-but-unaddressed peer
must be excluded everywhere a dial or a sleep deadline is computed: `nextDeadline`
would otherwise return 0 for it forever, `itsPoll(0)` returns instantly, and the
loop spins into the WDT.

### 5.3 Config reload (`reloadPeers`)

On any `s.tcp.peers` / `secrets.tcp.peers` change the task rebuilds `s_peers`,
matching old peers to new array entries by exact `host:port` so an unrelated
edit doesn't drop a live connection. For a matched peer it refreshes all fields
via `loadPeerConfig`, then forces a redial only when something that's baked into
the rnsd registration changed:

- enable→disable: disconnect, go `PS_IDLE`.
- disable→enable: arm an immediate `next_attempt_tick`.
- **mode or any IFAC field changed while up/connecting:** disconnect and redial,
  because mode and IFAC are captured into `rnsd_iface_t` at register time — the
  only way to apply a new value is to re-register the interface.

Unmatched old peers are disconnected (`"removed"`). New entries get a fresh
`runtime_id` and an immediate attempt.

### 5.4 The net-upstream gate

A peer only dials while there is a real STA upstream. The task tracks the edge
via `NET_EV_UPSTREAM_UP` / `NET_EV_UPSTREAM_DOWN` — **not** `NET_EV_UP`, which
also fires for AP-start, where there is no route off-device. `s_upstreamUp` is
seeded from `netIsStaConnected()` then driven by the events (which only flip the
`volatile` flag and notify; the actual teardown/redial happens on the tcp task
in `servicePeers`). On an upstream-up edge `s_netEdge` is set so the loop clears
accrued backoff and redials promptly instead of waiting it out.

## 6. Inbound TCP server

One listener serves all inbound connections; `s_inbound[TCP_MAX_INBOUND]`
(PSRAM, 8 slots) holds the accepted ones. The ITS server port `TCP_PORT_INBOUND`
(0x5443) is opened unconditionally at boot so config can flip the server on
later, but the TCP listen socket is only registered with `net` when
`s.tcp.server_enable` is set.

`serverRegister()` sends a `net_port_msg_t` on `NET_PORT_REG_PORT` with a
non-zero `tcpPort` (so net binds `s.tcp.server_port` directly, not via
`s.net.*`). Changing the port after registration needs a reboot; enable/disable
is handled purely by accepting or refusing connections.

`onInboundConnect` allocates the lowest free slot (refusing with `-1` when the
server is disabled or at `s_maxInbound`), reads the client IP from the
`net_connect_t` connect payload, and registers the interface with `rnsd` as
`tcp_in/<addr>#<slot>` carrying the server mode + IFAC. **The slot index is both
the serverRef handed to net and the ref handed to rnsd**, so either disconnect
callback resolves the same slot. Traffic uses the same `hdlcConsume`/`hdlcSend`
helpers as outbound.

`reconcileServer()` runs on config change: it registers the listener if newly
enabled, and — because mode/IFAC are baked at accept time — drops live inbound
connections when the server is disabled or its mode/IFAC changes, forcing
re-registration on reconnect.

## 7. IFAC

iface-tcp reads three values per interface — `ifac_netname` (`s.*`),
`ifac_netkey` passphrase (`secrets.*`), and `ifac_size` (`s.*`) — and copies
them into `rnsd_iface_t.{ifac_netname,ifac_netkey,ifac_size}` at register time.
That is the whole of its IFAC involvement.

The actual IFAC transform — deriving the per-interface key, masking and signing
outbound packets, and verifying/unmasking/dropping inbound ones byte-compatibly
with upstream Reticulum — lives in `rnsd` and the microReticulum fork. See
[rns INTERNALS](../rns/INTERNALS.md). A change to any IFAC field re-registers
the interface (§5.3 / §6) because the key is captured at registration.

## 8. Command sentinels

`tcp.cmd.connect` / `disconnect` / `restart` / `del` are storage keys the task
subscribes to; each handler runs on the tcp task, acts, then `storageUnset`s its
own key and ignores the resulting self-unset re-fire (`val == ""`). `connect`
and `disconnect` carry a slot number; `restart` tears down everything; `del`
removes a peer slot. Because `s.tcp.peers` is a JSON array, deleting an element
(via `storageUnset` of the whole element, not `storageDeleteTree`) compacts the
remaining entries down and fires the `s.tcp.peers` subscription — so a removal
reflows the array and refreshes every UI. `del` compacts the parallel
`secrets.tcp.peers` array in step.

`tcp start`/`stop` and `tcp server start`/`stop` write `s.tcp.enable` /
`s.tcp.server_enable` directly (not sentinels); the global gate change is
handled by `onGlobalEnableChange`, which tears down or brings back all peers.

## 9. Telemetry publishing

`publishPeerState(p)` writes `tcp.peers.<id>.{up,state,stats.tx_bytes,stats.rx_bytes}`.
State transitions publish immediately from `attemptConnect`/`disconnectPeer`;
the loop's periodic pass only refreshes the byte counters, and is **gated to
~1 Hz** (`s_nextPublishTick`). Publishing unconditionally is a trap: a busy peer
makes `itsPoll` return early on every RX, so committing the ever-changing byte
counts every iteration churns cJSON nonstop on CPU0 (task WDT) and fires
change-subscriptions faster than subscribers drain them. The 1 s cap in
`nextDeadline` guarantees the gated pass still runs when idle.

## 10. On-device LCD pane

`tcp_lcd.cpp` builds the Settings → Mesh Network → RNS Interfaces → TCP pane via
`lcdRegisterSettings`, registered through the `when: spangap/spangap-lcd`-gated
`tcpLcdRegister` init hook (so no `#if` is needed — the file is only compiled
when spangap-lcd is staged). The pane mirrors the web editor: static rows bind
`s.tcp.server_*` directly; the dynamic peer list adds/toggles via `s.tcp.peers.*`
writes and deletes via the `tcp.cmd.del` sentinel. It subscribes to
`s.tcp.peers` to rebuild the list, hopping onto the lcd task (`lcdRun`) to touch
LVGL, and nulls its widget pointers on pane delete so a late storage callback
can't touch freed objects.

## 11. Pitfalls

- **Drain ITS recv before any early return.** `rnsd` floods every interface with
  the announce stream; returning without `itsRecv` leaves bytes buffered and ITS
  redispatches forever → busy spin → task WDT.
- **Use `runtime_id`, not the array index, as the ITS ref.** `peer_t::id` floats
  with each reload; only `runtime_id` is stable, so disconnect callbacks must
  resolve via `peerByRuntimeId`.
- **Mode and IFAC are baked at register time.** They live in the `rnsd_iface_t`
  sent once at connect; the only way to change them on a live interface is to
  disconnect and re-register. `reloadPeers`/`reconcileServer` do exactly that.
- **Gate dialing on `NET_EV_UPSTREAM_UP`, not `NET_EV_UP`.** AP-only mode raises
  `NET_EV_UP` but has no route off-device; dialing there burns connect/teardown
  cycles and defeats light sleep.
- **An enabled, unaddressed peer must be excluded from the dial/deadline logic.**
  Otherwise `nextDeadline` returns 0 for it, `itsPoll(0)` never blocks, and the
  task spins into the WDT. `peerDialable` is the single predicate; keep every
  dial/sleep decision behind it.
- **Keep stats publishing gated to ~1 Hz.** Unconditional publishing under a
  busy peer churns cJSON on CPU0 and overruns change-subscriptions.
- **Recv buffers are `static`, never `thread_local`.** ITS dispatches a handle's
  callbacks only on its registering task, so a plain static is safe; `thread_local`
  pulls in libgcc lazy TLS init that has corrupted scheduler state at boot. The
  same task affinity is why no locking guards `s_peers`/`s_inbound`.
- **`s.tcp.version` is a config-version gate** used to seed the server-side
  defaults once. The project's no-migrations policy makes such gates a code
  smell; it is not a documented tunable.
