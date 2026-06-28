# iface-tcp — RNS over TCP

**iface-tcp** carries Reticulum traffic over plain TCP/IP. It dials outbound
TCP peers and accepts inbound TCP connections, frames RNS packets on the wire,
and registers each connection as its own interface with [rns](../rns)'s `rnsd`.
It is one of the interface straddles that plug into Reticulum's Transport; the
others are [iface-lora](../iface-lora), [iface-espnow](../iface-espnow), and
[iface-auto](../iface-auto).

## Origins

The wire format and interface roles follow upstream Reticulum's TCP interfaces:
an outbound dial is a **TCPClientInterface**, the listener is a
**TCPServerInterface**, and packets are delimited with the same HDLC
byte-stuffing the upstream Python `TCPInterface` uses. iface-tcp itself owns no
sockets — it borrows TCP from [spangap-net](../spangap-net) and the protocol
engine from [rns](../rns). See [INTERNALS.md](INTERNALS.md) for the framing and
lifecycle detail.

## What it does

iface-tcp runs one FreeRTOS task that bridges two ITS services: [spangap-net](../spangap-net)
(the TCP byte stream) and [rns](../rns)'s `rnsd` (the RNS packet stream). Each
TCP connection — whether dialed out or accepted in — becomes a distinct RNS
interface:

```
                        ┌──── tcp task ────┐
  net  ◄── TCP bytes ──►│  HDLC framing    │◄── RNS packets ──►  rnsd
  (NET_PORT_TCP_DIAL /  │  per connection  │   (RNSD_PORT_IFACE)
   NET_PORT_REG_PORT)   └──────────────────┘
```

For an outbound peer the task dials `net` (`NET_PORT_TCP_DIAL` with a
`"host:port"` payload), and on success opens an ITS connection to `rnsd`'s
`RNSD_PORT_IFACE` carrying an `rnsd_iface_t` that names the interface
`tcp/<id>` and describes its MTU, bitrate, mode, and IFAC credentials. After
that the two ITS handles are the packet pipe: bytes off the socket are
de-framed and handed to `rnsd` as RNS packets; packets from `rnsd` are
HDLC-framed and written to the socket. A dropped TCP connection deregisters the
interface and the peer reconnects with exponential backoff.

The inbound listener registers a TCP port with `net` (`NET_PORT_REG_PORT`);
each accepted client becomes an interface named `tcp_in/<addr>#<slot>`, framed
identically and carrying the server's mode and IFAC.

It starts automatically when the straddle is in the build — `tcpInit` is wired
into the generated init dispatcher, and the task waits on `rns.ready` before
dialing or listening. There is no compile-time peer list; everything is driven
from storage at runtime.

### Interface modes and IFAC

Every interface carries a Reticulum **mode** (`full`, `gateway`,
`access_point`, `roaming`, `boundary`; default `gateway`) that governs how
Transport treats its traffic, and optionally **IFAC** (Interface Access Codes)
— a per-interface shared secret (`network_name` + passphrase) that masks and
authenticates every packet so only nodes sharing the same credentials
interoperate. IFAC is configured per interface: each outbound peer and the
inbound server can join a different IFAC network, or stay open. The crypto
enforcement lives in `rnsd` / microReticulum (see [rns](../rns)); iface-tcp only
reads the credentials from storage and passes them to `rnsd` at register time.

## Ports it uses

iface-tcp is a consumer — it exposes no ITS ports of its own. It connects to:

| Service | Port | Use |
|---|---|---|
| `rnsd` | `RNSD_PORT_IFACE` | Register each connection as an RNS interface; the handle is the packet pipe. |
| `net` | `NET_PORT_TCP_DIAL` | Dial an outbound peer (`"host:port"` payload). |
| `net` | `NET_PORT_REG_PORT` | Register the inbound listen port. |

Exact struct layouts: `rnsd_iface_t` and `rns_iface_mode` in
[rns `ports.h`](../rns/esp-idf/include/ports.h); `net_port_msg_t` /
`net_connect_t` in [net.h](../spangap-net/esp-idf/include/net.h).

## Storage variables

Configuration is entirely storage-driven. Outbound peers live in the JSON array
`s.tcp.peers`; the inbound server and global gate live under `s.tcp.*`.
Passphrases are secrets (`secrets.*`), which persist on-device but never sync to
the browser.

### Settings (read)

**Global**

| Key | Default | Meaning |
|---|---|---|
| `s.tcp.enable` | `1` | Global gate. When `0`, every peer is disconnected regardless of per-peer enable. Live (no reboot). |

**Per outbound peer** — `s.tcp.peers.<id>.*` (array index `<id>`)

| Key | Default | Meaning |
|---|---|---|
| `enable` | `0` | Per-peer on/off. (`tcp peer add` writes `1`.) |
| `host` | `""` | Hostname or IPv4 of the peer's TCP server. |
| `port` | `4965` | TCP port (Reticulum's default). |
| `mode` | `gateway` | Interface mode: `full`/`gateway`/`access_point`/`roaming`/`boundary`. |
| `ifac_netname` | `""` | IFAC network name. `""` = open interface. |
| `ifac_size` | `0` | IFAC access-code length in bytes. `0` = default (1). |
| `retry_min` | `2` | Reconnect backoff floor, seconds. |
| `retry_max` | `300` | Reconnect backoff ceiling, seconds (clamped to ≥ `retry_min`). |

**Inbound server** — `s.tcp.*`

| Key | Default | Meaning |
|---|---|---|
| `s.tcp.server_enable` | `0` | Accept inbound TCP connections. |
| `s.tcp.server_port` | `4965` | Listen port. Changing it after boot needs a reboot. |
| `s.tcp.server_mode` | `gateway` | Mode applied to every accepted interface. |
| `s.tcp.max_inbound` | `8` | Concurrent inbound connection cap (hard ceiling 8). |
| `s.tcp.server_ifac_netname` | `""` | IFAC network name for accepted connections. |
| `s.tcp.server_ifac_size` | `0` | IFAC access-code length. `0` = default (1). |

### Secrets

| Key | Meaning |
|---|---|
| `secrets.tcp.peers.<id>.ifac_netkey` | IFAC passphrase for outbound peer `<id>`. `""` = open. |
| `secrets.tcp.server_ifac_netkey` | IFAC passphrase for the inbound server. `""` = open. |

### Runtime telemetry (written)

Per outbound peer, refreshed at ~1 Hz:

| Key | Meaning |
|---|---|
| `tcp.peers.<id>.up` | `1` when the peer is `up`, else `0`. |
| `tcp.peers.<id>.state` | `idle` / `connecting` / `up` / `backoff`. |
| `tcp.peers.<id>.stats.tx_bytes` | Bytes sent to the peer (cumulative). |
| `tcp.peers.<id>.stats.rx_bytes` | Bytes received from the peer (cumulative). |

Inbound connections are not mirrored to storage; their state and byte counters
are shown by `tcp` (CLI) only.

### Command sentinels (write, self-clearing)

Single-shot triggers the tcp task consumes and unsets:

| Key | Value | Effect |
|---|---|---|
| `tcp.cmd.connect` | slot | Force-connect a peer, clearing its backoff. |
| `tcp.cmd.disconnect` | slot | Drop a peer's connection (auto-reconnect still applies). |
| `tcp.cmd.restart` | any | Tear down every peer connection; enabled peers redial. |
| `tcp.cmd.del` | slot | Remove a peer slot (compacts the array). |

## CLI

```
tcp                               list peers + inbound-server status
tcp start | stop | restart        global gate (s.tcp.enable) / redial all
tcp server [start|stop]           inbound TCP listener (s.tcp.server_enable)
tcp connect <slot>                force-connect a peer (clears backoff)
tcp disconnect <slot>             kick a peer's connection
tcp peer add <host[:port]> [mode] add a peer (port 4965, mode gateway)
tcp peer rm <slot>                remove a peer slot
tcp peer enable <slot>            persistently enable a peer
tcp peer disable <slot>           persistently disable a peer
tcp peer mode <slot> <mode>       full|gateway|access_point|roaming|boundary
```

`tcp disconnect` is an ad-hoc kick: the peer goes to backoff and reconnects if
still enabled. `tcp peer disable` is persistent — it writes
`s.tcp.peers.<slot>.enable = 0`, survives reboot, and stops auto-reconnect.

Run any of these on-device with `spangap cli "<command>"`.

## Browser & on-device UI

- **Browser:** `panels/TcpPanel.vue` (Settings → Mesh → RNS Interfaces → TCP) —
  a per-peer editor (host/port/enable/mode + IFAC), drag-to-reorder of the peer
  list, the inbound-server section, and live per-peer state badges. Registered
  via `modules/tcp.ts`.
- **On-device LCD:** `conditional/spangap-lcd/src/tcp_lcd.cpp` adds the same
  pane to the device's own settings (Mesh Network → RNS Interfaces → TCP),
  compiled only when [spangap-lcd](../spangap-lcd) is in the build. Edits from
  the LCD, the browser, and the CLI all drive the same `s.tcp.*` storage.

## Dependencies

- [rns](../rns) — the RNS protocol engine; iface-tcp registers interfaces with
  its `rnsd` and depends on it being topologically ahead in the init order.
- [spangap-net](../spangap-net) — TCP dial + listen, and the upstream-up edge
  events the task gates dialing on.

## Read next

- [INTERNALS.md](INTERNALS.md) — the task model, HDLC framing, peer lifecycle
  and reconnect, the inbound server, IFAC plumbing, and maintainer pitfalls.
