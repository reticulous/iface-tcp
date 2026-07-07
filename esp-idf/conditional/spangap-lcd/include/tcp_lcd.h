/**
 * tcp_lcd.h — the TCP interface's on-device Settings pane, as a Service.
 *
 * TcpLcdService registers the hand-written "Mesh Network/RNS Interfaces/TCP"
 * settings pane (a peers-list editor too rich for a declarative settings:
 * block). It is when:-gated on spangap-lcd via the straddle.yaml `services:`
 * entry, so it exists only in an LCD build. Declared here (global) for the
 * generated trampoline; defined in tcp_lcd.cpp where the pane builder lives.
 */
#pragma once

#include "service.h"

class TcpLcdService : public Service {
public:
    void onInit() override;
};
