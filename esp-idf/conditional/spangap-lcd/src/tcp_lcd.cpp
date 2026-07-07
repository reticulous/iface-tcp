/**
 * tcp_lcd.cpp — on-device Settings pane for the TCP transport.
 *
 * Settings → Mesh Network → RNS Interfaces → TCP. Inbound-server settings (static
 * rows) plus a dynamic outbound-peer editor: each peer listed with an enable
 * toggle and a delete, and an add form (host / port / mode). Modeled on the WiFi
 * known-networks pane (net_lcd.cpp).
 *
 * Peer mutations drive the same storage the web editor and CLI do: add/toggle
 * write s.tcp.peers.* directly (the tcp task reloads on the change); delete routes
 * through the tcp.cmd.del sentinel (it compacts the contiguous array and reloads).
 *
 * Compiled only when spangap-lcd is staged (conditional/spangap-lcd/), via the
 * when:-gated tcpLcdRegister init hook. No #if needed.
 */
#include "lcd.h"
#include "storage.h"
#include "tcp_lcd.h"

#include <string>
#include <cstdio>
#include <cstdlib>
#include <cstdint>

namespace {

constexpr int kMaxPeers = 16;   /* mirrors TCP_MAX_PEERS in tcp.cpp */
const char*   kModeOpts = "gateway\nfull\naccess_point\nroaming\nboundary";

/* Nulled on pane delete so a late storage callback can't touch freed objects. */
lv_obj_t* s_peerBox = nullptr;
lv_obj_t* s_hostTa  = nullptr;
lv_obj_t* s_portTa  = nullptr;
lv_obj_t* s_modeDD  = nullptr;
bool      s_subscribed = false;

std::string peerField(int idx, const char* field) {
  char k[64];
  snprintf(k, sizeof(k), "s.tcp.peers.%d.%s", idx, field);
  return storageGetStr(k, "");
}

lv_obj_t* mkRow(lv_obj_t* parent) {
  lv_obj_t* r = lv_obj_create(parent);
  lv_obj_remove_style_all(r);
  lv_obj_set_width(r, lv_pct(100));
  lv_obj_set_height(r, LV_SIZE_CONTENT);
  lv_obj_set_flex_flow(r, LV_FLEX_FLOW_ROW);
  lv_obj_set_flex_align(r, LV_FLEX_ALIGN_SPACE_BETWEEN, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
  lv_obj_set_style_pad_hor(r, 8, 0);
  lv_obj_set_style_pad_ver(r, 3, 0);
  lv_obj_set_style_pad_column(r, 6, 0);
  lv_obj_remove_flag(r, LV_OBJ_FLAG_SCROLLABLE);
  return r;
}

lv_obj_t* mkBtn(lv_obj_t* parent, const char* txt, lv_event_cb_t cb, void* ud) {
  lv_obj_t* b = lv_button_create(parent);
  lv_obj_set_style_pad_hor(b, 8, 0);
  lv_obj_set_style_pad_ver(b, 4, 0);
  lv_obj_t* l = lv_label_create(b);
  lv_label_set_text(l, txt);
  lv_obj_center(l);
  lv_obj_add_event_cb(b, cb, LV_EVENT_CLICKED, ud);
  if (lcdInputGroup()) lv_group_add_obj(lcdInputGroup(), b);
  return b;
}

lv_obj_t* mkField(lv_obj_t* parent, const char* placeholder, lv_obj_t** slot) {
  lv_obj_t* ta = lv_textarea_create(parent);
  lv_textarea_set_one_line(ta, true);
  lv_textarea_set_placeholder_text(ta, placeholder);
  lv_obj_set_width(ta, lv_pct(100));
  if (lcdInputGroup()) lv_group_add_obj(lcdInputGroup(), ta);
  *slot = ta;
  return ta;
}

void onTogglePeer(lv_event_t* e) {
  int idx = (int)(intptr_t)lv_event_get_user_data(e);
  char k[64]; snprintf(k, sizeof(k), "s.tcp.peers.%d.enable", idx);
  storageSet(k, storageGetInt(k, 1) ? 0 : 1);   /* fires s.tcp.peers -> reload + rebuild */
}

void onDelPeer(lv_event_t* e) {
  int idx = (int)(intptr_t)lv_event_get_user_data(e);
  char v[8]; snprintf(v, sizeof(v), "%d", idx);
  storageSet("tcp.cmd.del", v);   /* tcp task compacts the array + reloads */
}

void onAddPeer(lv_event_t*) {
  if (!s_hostTa) return;
  std::string host = lv_textarea_get_text(s_hostTa);
  if (host.empty()) return;
  std::string portStr = s_portTa ? lv_textarea_get_text(s_portTa) : "";
  int port = portStr.empty() ? 4965 : atoi(portStr.c_str());
  if (port <= 0 || port > 65535) port = 4965;
  char mode[24] = "gateway";
  if (s_modeDD) lv_dropdown_get_selected_str(s_modeDD, mode, sizeof(mode));

  /* Lowest free slot (matches cliTcpPeerAdd). */
  int slot = -1;
  for (int i = 0; i < kMaxPeers; i++) {
    char k[64]; snprintf(k, sizeof(k), "s.tcp.peers.%d.host", i);
    if (!storageExists(k)) { slot = i; break; }
  }
  if (slot < 0) return;   /* full */

  /* Keep s.tcp.peers a JSON array: the first peer establishes it via setTree
   * (nothing to preserve); later peers merge-append as array element `slot`.
   * This is what makes device-added peers show up in the web list. */
  if (slot == 0) {
    cJSON* arr = cJSON_CreateArray();
    cJSON* o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "enable", 1);
    cJSON_AddStringToObject(o, "host", host.c_str());
    cJSON_AddNumberToObject(o, "port", port);
    cJSON_AddStringToObject(o, "mode", mode);
    cJSON_AddItemToArray(arr, o);
    storageSetTree("s.tcp.peers", arr);
  } else {
    char k[64];
    storageBegin();
    snprintf(k, sizeof(k), "s.tcp.peers.%d.host",   slot); storageSet(k, host.c_str());
    snprintf(k, sizeof(k), "s.tcp.peers.%d.port",   slot); storageSet(k, port);
    snprintf(k, sizeof(k), "s.tcp.peers.%d.mode",   slot); storageSet(k, mode);
    snprintf(k, sizeof(k), "s.tcp.peers.%d.enable", slot); storageSet(k, 1);
    storageEnd();   /* fires s.tcp.peers -> reload + rebuild */
  }

  lv_textarea_set_text(s_hostTa, "");
  if (s_portTa) lv_textarea_set_text(s_portTa, "");
}

void rebuildPeers() {
  if (!s_peerBox) return;
  lv_obj_clean(s_peerBox);
  int n = storageArrayCount("s.tcp.peers");
  if (n <= 0) {
    lv_obj_t* l = lv_label_create(s_peerBox);
    lv_label_set_text(l, "  (no peers)");
    lv_obj_set_style_text_color(l, lv_color_hex(0x8a93a0), 0);
    return;
  }
  for (int i = 0; i < n; i++) {
    std::string host = peerField(i, "host");
    if (host.empty()) continue;
    std::string port = peerField(i, "port");
    std::string mode = peerField(i, "mode");
    char ek[64]; snprintf(ek, sizeof(ek), "s.tcp.peers.%d.enable", i);
    bool en = storageGetInt(ek, 1) != 0;
    char sk[64]; snprintf(sk, sizeof(sk), "tcp.peers.%d.state", i);
    std::string state = storageGetStr(sk, "");

    lv_obj_t* r = mkRow(s_peerBox);
    lv_obj_t* lbl = lv_label_create(r);
    std::string txt = host + ":" + (port.empty() ? "4965" : port);
    if (!mode.empty())  txt += "  " + mode;
    if (!state.empty()) txt += "  \xE2\x80\x94 " + state;   /* em-dash */
    lv_label_set_text(lbl, txt.c_str());
    lv_obj_set_style_text_color(lbl, en ? lv_color_white() : lv_color_hex(0x8a93a0), 0);
    lv_label_set_long_mode(lbl, LV_LABEL_LONG_DOT);
    lv_obj_set_flex_grow(lbl, 1);
    mkBtn(r, en ? "On" : "Off", onTogglePeer, (void*)(intptr_t)i);
    mkBtn(r, "Del", onDelPeer, (void*)(intptr_t)i);
  }
}

/* Storage-change handler: hop onto the lcd task to touch LVGL safely regardless
 * of which task the storage actor dispatches from. Guarded by the nulled-on-
 * delete s_peerBox pointer inside rebuildPeers(). */
void onTcpStorage(const char* /*key*/, const char* /*val*/) {
  lcdRun(ON_LCD { rebuildPeers(); });
}

void onPaneDelete(lv_event_t*) {
  s_peerBox = s_hostTa = s_portTa = s_modeDD = nullptr;
  if (s_subscribed) {
    /* This prefix is ours alone here (the static rows bind s.tcp.server_*, a
     * different scope), so a scope unsubscribe is safe. */
    storageUnsubscribe("s.tcp.peers");
    s_subscribed = false;
  }
}

void tcpSettingsPane(void* arg) {
  lv_obj_t* p = static_cast<lv_obj_t*>(arg);

  lcdSettingSection (p, "Incoming");
  lcdSettingSwitch  (p, "Enabled",     "s.tcp.server_enable");
  lcdSettingText    (p, "Listen port", "s.tcp.server_port");
  lcdSettingDropdown(p, "Mode",        "s.tcp.server_mode", "full,gateway,access_point,roaming,boundary");
  lcdSettingText    (p, "Max inbound", "s.tcp.max_inbound");

  lcdSettingSection(p, "Peers");
  s_peerBox = lv_obj_create(p);
  lv_obj_remove_style_all(s_peerBox);
  lv_obj_set_width(s_peerBox, lv_pct(100));
  lv_obj_set_height(s_peerBox, LV_SIZE_CONTENT);
  lv_obj_set_flex_flow(s_peerBox, LV_FLEX_FLOW_COLUMN);
  lv_obj_remove_flag(s_peerBox, LV_OBJ_FLAG_SCROLLABLE);

  lcdSettingSection(p, "Add peer");
  mkField(p, "Host", &s_hostTa);
  mkField(p, "Port (4965)", &s_portTa);
  s_modeDD = lv_dropdown_create(p);
  lv_dropdown_set_options(s_modeDD, kModeOpts);
  lv_obj_set_width(s_modeDD, lv_pct(100));
  if (lcdInputGroup()) lv_group_add_obj(lcdInputGroup(), s_modeDD);
  mkBtn(p, "Add", onAddPeer, nullptr);

  rebuildPeers();

  if (!s_subscribed) {
    storageSubscribeChanges("s.tcp.peers", onTcpStorage);
    s_subscribed = true;
  }
  /* Tear the subscription down when the pane is destroyed. */
  lv_obj_add_event_cb(p, onPaneDelete, LV_EVENT_DELETE, nullptr);
}

}  // namespace

/* The TCP settings-pane service (when:-gated on spangap-lcd via straddle.yaml
 * services:). onInit registers the hand-written pane; the whole TU is compiled
 * only under conditional/spangap-lcd/, so no #if is needed. */
void TcpLcdService::onInit() {
    lcdRegisterSettings("Mesh Network/RNS Interfaces/TCP", "TCP", tcpSettingsPane);
}
