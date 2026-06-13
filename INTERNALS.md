# iface-tcp — internals

## Task

One transport task, registered with rnsd at `tcpInit()` time via
`RNSD_PORT_REGISTER`. Subscribes to `s.tcp.enable` + `s.tcp.peers` and
brings itself up / tears itself down accordingly.

## Wire framing

HDLC byte-stuffing on the wire. Frame boundary is `0x7E`; literal
`0x7E` in the payload escapes to `0x7D 0x5E`, literal `0x7D` escapes to
`0x7D 0x5D`. Same framing as upstream RNS TCPInterface.

## Iface lifecycle

Each peer = one iface. Per peer:

- On enable / dial-success: register a fresh iface name with rnsd via
  `RNSD_PORT_REGISTER`.
- Stream bytes are HDLC-framed and handed to rnsd over the per-peer
  `RNSD_PORT_TRANSPORT` connection (one ITS connection per direction).
- On disconnect / dial failure: deregister, schedule a reconnect with
  exponential backoff capped at e.g. 60 s.

Dialing is gated on a real STA upstream (`NET_EV_UPSTREAM_UP/DOWN`,
seeded from `netIsStaConnected()`) — not bare `NET_EV_UP`. In AP-only
mode there's no route off-device, so peers stay idle and existing
connections are torn down until an upstream returns.

Inbound listens (currently Phase 1: only outbound; inbound listen lands
in Phase 5) register the listen port with `net` via
`NET_PORT_REG_PORT` and accept new connections as their own peers.

## Status

- Phase 1: outbound dial — implemented and hardware-verified.
- Phase 5: inbound listen — planned.

## Browser

`panels/TcpPanel.vue` exposes per-peer enable/dial-target + a live
iface-status table; `modules/tcp.ts` subscribes to a `tcp:1`
DataChannel for per-iface stats.
