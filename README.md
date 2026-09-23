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
| `s.tcp.announce_interval` | `30` | How often this node says who it is: rnsd replays every hosted destination's announce onto **every** connection this straddle holds on this beat, jittered ±10 %. Minutes. One setting for all of them, outbound `tcp/<n>` and inbound `tcp_in/<addr:port>` alike — they are the same medium, and a connection is not a thing an operator wants to schedule separately. A connection made mid-interval is told who we are as it registers. `0` = never on this interface's own account. Live. See [rns/README.md](../rns/README.md), "The announce tick". |

**Per outbound peer** — `s.tcp.peers.<id>.*` (array index `<id>`)

| Key | Default | Meaning |
|---|---|---|
| `enable` | `0` | Per-peer on/off. (`tcp peer add` writes `1`.) |
| `name` | `""` | Display name shown in the peer list; `host:port` when empty. |
| `host` | `""` | Hostname or IPv4 of the peer's TCP server. |
| `port` | `4965` | TCP port (Reticulum's default). |
| `mode` | `access_point` | Interface mode: `full`/`gateway`/`access_point`/`roaming`/`boundary`. |
| `ifac_netname` | `""` | IFAC network name. `""` = open interface. |
| `ifac_size` | `0` | IFAC access-code length in bytes. `0` = default (1). |
| `community_radius` | `0` | Service radius: nodes within this many hops via this peer are served — their announces kept and answered for, searches run on their behalf. `0` (default) treats the peer as an uplink: a TCP peer into the wider network delivers everyone's announces, unbounded, so rnsd keeps only what was resolved on demand, claimed, or is in active use. Raise it for a peer fronting a segment this node should serve. See `rns/README.md`. |
| `retry_min` | `2` | Reconnect backoff floor, seconds. |
| `retry_max` | `300` | Reconnect backoff ceiling, seconds (clamped to ≥ `retry_min`). |

**Incoming Ports** — `s.tcp.servers.<i>.*` (a collection like the peers; up to 4 listeners)

| Key | Default | Meaning |
|---|---|---|
| `enable` | `1` | Per-port on/off. Live: enabling opens the listen socket, disabling closes it (no reboot). |
| `port` | `4965` | Listen port. Live: changing it re-binds the socket (no reboot). Must be unique among the listeners. |
| `upnp` | `1` | **Accessible from internet** — ask the router to forward this port in from the WAN at the same external port, so a Reticulum node outside the LAN can dial it. The flag rides the port's registration with [spangap-net](../spangap-net) as `publicFacing`; [upnp](../upnp) is what acts on it, and the switch appears in the pane only in a build that stages upnp. On by default: a listen port is there to be dialed. |
| `mode` | `access_point` | Mode applied to every interface accepted on this port. |
| `max_conns` | `16` | Concurrent connection cap for this port (hard ceiling 16 across all ports). |
| `community_radius` | `0` | Service radius for callers on this port; `0` (default) treats them as uplinks. |
| `ifac_netname` | `""` | IFAC network name for accepted connections. |
| `ifac_size` | `0` | IFAC access-code length. `0` = default (1). |

### Secrets

| Key | Meaning |
|---|---|
| `s.tcp.peers.<n>.ifac_netkey` | IFAC passphrase for that outbound peer. `""` = open. An ordinary field of the item, masked where it is shown — a code the other end was given is one an operator has to be able to read back. |
| `s.tcp.servers.<n>.ifac_netkey` | IFAC passphrase for that incoming port. `""` = open. |

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

And one key for the whole class:

| Key | Meaning |
|---|---|
| `rns.pill.tcp.*` | The top status line's TCP pill (red `ff5555`, order 3), written through rnsd: `T` and how many connections this node holds, outbound and inbound added together — on this medium a peer IS a connection, and the two directions are one interface class from a status line's point of view. Shown only while at least one **outbound peer** is configured and enabled: the global gate defaults on and a listen port is passive, so keying the pill on either would put a permanent `T0` on every node in the fleet, most of which never dial anything. An enabled outbound peer is somebody stating an intention to be connected — and `T0` then says exactly the thing worth saying: configured, and not coming up. See [rns/README](../rns/README.md#status-line-pills). |

### Command keys (write, self-clearing)

Single-shot triggers the tcp task consumes and unsets:

| Key | Value | Effect |
|---|---|---|
| `tcp.cmd.connect` | slot | Force-connect a peer, clearing its backoff. |
| `tcp.cmd.disconnect` | slot | Drop a peer's connection (auto-reconnect still applies). |
| `tcp.cmd.restart` | any | Tear down every peer connection; enabled peers redial. |
| `tcp.cmd.del` | slot | Remove a peer slot (compacts the array). |
| `tcp.announce_now` | any | Ask rnsd to replay every hosted destination's announce onto every TCP connection, in and out now — the pane's **Announce now** button. |

## CLI

```
tcp                               list peers + incoming-port status
tcp start | stop | restart        global gate (s.tcp.enable) / redial all
tcp server                        incoming-port status (s.tcp.servers)
tcp connect <slot>                force-connect a peer (clears backoff)
tcp disconnect <slot>             kick a peer's connection
tcp peer add <host[:port]> [mode] add a peer (port 4965, mode access_point)
tcp peer rm <slot>                remove a peer slot
tcp peer enable <slot>            persistently enable a peer
tcp peer disable <slot>           persistently disable a peer
tcp peer mode <slot> <mode>       full|gateway|access_point|roaming|boundary
tcp n[eighbors] [-v]              direct RNS peers, per connection
```

`tcp` lists the CONNECTIONS; `tcp n` lists who is one hop away over them —
inbound connections included, since they are the same medium. Each connection is
its own point-to-point interface, so the connection **is** the node: one numbered
block per connection with its destinations under it, labelled by the address
(`peer_label`, the dialled `host:port` or the accepted peer's address) rather
than by the registration name, which is a slot number. The table is rnsd's,
shared by every interface straddle, so a peer on a radius-0 connection is
deliberately not tracked: an uplink's far end is a route, not a neighbourhood,
and `n` says so rather than showing an empty list. See
[rns/README](../rns/README.md#the-neighbourhood--who-is-one-hop-away).

`tcp disconnect` is an ad-hoc kick: the peer goes to backoff and reconnects if
still enabled. `tcp peer disable` is persistent — it writes
`s.tcp.peers.<slot>.enable = 0`, survives reboot, and stops auto-reconnect.

Run any of these on-device with `spangap cli "<command>"`.

## Settings UI

**Settings → Reticulum Mesh → TCP** is described once, by the
`settings:` block in `straddle.yaml`, and the build lowers it to the browser and
to the display: the peer collection (per-peer editor for host/port/enable/mode,
announce retention, IFAC and retry backoff, drag or up/down reorder, live status
pills) and the inbound-server section, identical on both. The add form asks for
host and port only; everything else about a peer is set in its editor.

The UI never writes `s.tcp.peers`. Every mutation is a `tcp.peer.*` command key and
`tcp.cpp` is the array's only writer, so the host and port checks exist once and
a rejection comes back as a sentence on `tcp.peer.error`. Each peer carries an
`id` that survives the compaction a delete performs, because the collection
addresses items by it. Edits from the display, the browser and the CLI all end
up in the same `s.tcp.*` storage.

## Dependencies

- [rns](../rns) — the RNS protocol engine; iface-tcp registers interfaces with
  its `rnsd` and depends on it being topologically ahead in the init order.
- [spangap-net](../spangap-net) — TCP dial + listen, and the upstream-up edge
  events the task gates dialing on.

## Read next

- [INTERNALS.md](INTERNALS.md) — the task model, HDLC framing, peer lifecycle
  and reconnect, the inbound server, IFAC plumbing, and maintainer pitfalls.
