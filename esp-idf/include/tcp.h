/**
 * tcp — TCP interface task (both outbound dial and inbound listen).
 *
 * Watches s.tcp.peers.* for configured outbound peers (dialed via
 * net's NET_PORT_TCP_DIAL) and s.tcp.server_* for inbound listen.
 * Each connection — dialed or accepted — becomes its own RNS interface
 * registered with rnsd via RNSD_PORT_IFACE.
 *
 * Wire format on TCP is HDLC-framed RNS packets (FLAG=0x7E, ESC=0x7D,
 * ESC_MASK=0x20). Same byte stuffing as the upstream Python
 * TCPInterface.
 */
#pragma once

void tcpInit(void);
