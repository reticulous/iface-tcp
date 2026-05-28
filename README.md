# reticulous-tcp

## What is this?

**reticulous-tcp** is the RNS-over-TCP transport for
[reticulous-core](../reticulous-core). It opens outbound TCP dials to
configured peers (via `spangap-net`'s `NET_PORT_TCP_DIAL`) and accepts
inbound listens, frames RNS packets with HDLC byte-stuffing on the
wire, and registers each peer as its own interface with `rnsd` through
`RNSD_PORT_TRANSPORT`.

## What this straddle owns

```
reticulous-tcp/
├── esp-idf/
│   ├── include/tcp.h
│   └── src/tcp.cpp        the tcp transport task
└── browser/
    └── src/
        ├── modules/tcp.ts        Pinia store + RPC
        └── panels/TcpPanel.vue   Settings → Transports → TCP
```

## How others use it

```cpp
tcpInit();    // after rnsdInit and netInit
```

Configuration:

- `s.tcp.enable` — on/off
- `s.tcp.peers` — list of `host:port` outbound dial targets
- `s.tcp.listen_port` — inbound TCP listen port (registered with
  `net` via `NET_PORT_REG_PORT`)

Each peer registers as its own interface with rnsd; the iface name is
`tcp.<host>:<port>`.

## Dependencies

- [reticulous-core](../reticulous-core) — protocol core.
- [spangap-net](../../s/spangap-net) — TCP dial + listen.

## Read next

- [INTERNALS.md](INTERNALS.md) — wire framing, reconnect backoff,
  iface lifecycle.
- The reticulous-tdeck doc:
  [docs/tcp.md](../reticulous-tdeck/docs/tcp.md).
