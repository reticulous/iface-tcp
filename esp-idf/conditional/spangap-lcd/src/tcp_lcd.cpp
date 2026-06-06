/**
 * tcp_lcd.cpp — on-device Settings pane for the TCP transport.
 *
 * Settings → Reticulum → Transports → TCP. The outbound peer list is an array
 * editor (add/remove/per-peer host+port) — that stays in the web UI for now,
 * like the WiFi known-networks list; here we surface the live peer states.
 *
 * This whole file lives under conditional/spangap-lcd/, compiled only when the
 * lcd straddle is staged, so no #if is needed. It registers via the when:-gated
 * tcpLcdRegister init hook (spangap/spangap-lcd).
 */
#include "lcd.h"

static void tcpSettingsPane(void* arg) {
    lv_obj_t* p = static_cast<lv_obj_t*>(arg);
    lcdSettingSection(p, "TCP peers");
    lcdSettingValue  (p, "Peer 0", "tcp.peers.0.state");
    lcdSettingValue  (p, "Peer 1", "tcp.peers.1.state");
    lcdSettingValue  (p, "Peer 2", "tcp.peers.2.state");
    lcdSettingValue  (p, "Peer 3", "tcp.peers.3.state");
    lv_obj_t* note = lv_label_create(p);
    lv_label_set_text(note, "Add / edit peers in the web UI.");
    lv_obj_set_style_text_color(note, lv_color_hex(0x8a93a0), 0);
}

/* when:-gated init: hook (spangap/spangap-lcd). Plain C++ linkage to match the
 * generated dispatcher's forward decl. */
void tcpLcdRegister(void) {
    lcdRegisterSettings("Reticulum/Transports/TCP", "TCP", tcpSettingsPane);
}
