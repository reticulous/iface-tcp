<template>
  <div class="q-gutter-y-md">
    <PanelHeading title="TCP" />

    <div class="text-caption" style="opacity:0.7">
      Outbound TCP peers — dial public RNS TCP servers or other Reticulum
      nodes by host:port. Each peer registers as its own RNS interface.
    </div>

    <div v-if="peers.length === 0" class="text-caption" style="opacity:0.5">No peers configured</div>

    <div
      v-for="(p, idx) in peers" :key="idx"
      class="peer-item"
      :class="{ selected: selectedIdx === idx }"
      draggable="true"
      @click="selectedIdx = idx"
      @dragstart="onDragStart(idx, $event)"
      @dragover.prevent="onDragOver(idx, $event)"
      @drop="onDrop(idx)"
      @dragend="dragIdx = -1"
    >
      <div class="peer-drag-handle">&#x2630;</div>
      <div class="peer-label">{{ p.host || '(no host)' }}:{{ p.port || '?' }}</div>
      <q-badge v-if="peerState(idx) === 'up'" color="green" align="middle">up</q-badge>
      <q-badge v-else-if="peerState(idx) === 'connecting'" color="orange" align="middle">connecting</q-badge>
      <q-badge v-else-if="peerState(idx) === 'backoff'" color="red" align="middle">backoff</q-badge>
    </div>

    <div class="row q-gutter-x-sm q-mt-xs">
      <q-btn dense no-caps label="+" class="peer-btn" @click="openAddDialog" />
      <q-btn dense no-caps label="&minus;" class="peer-btn" :disable="selectedIdx < 0" @click="removePeer" />
    </div>

    <q-dialog v-model="showAddDialog" persistent>
      <q-card dark class="q-pa-md" style="min-width:320px">
        <q-card-section>
          <div class="text-subtitle1 text-weight-medium">Add Peer</div>
        </q-card-section>
        <q-card-section class="q-gutter-y-sm">
          <q-input :model-value="newHost" label="Host" dense outlined autofocus
            autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"
            @update:model-value="onHostInput"
            @keyup.enter="confirmAdd" />
          <q-input v-model.number="newPort" label="Port" type="number" dense outlined
            @keyup.enter="confirmAdd" />
        </q-card-section>
        <q-card-actions align="right">
          <q-btn flat label="Cancel" @click="showAddDialog = false" />
          <q-btn flat label="Add" color="primary" :disable="!newHost.trim()" @click="confirmAdd" />
        </q-card-actions>
      </q-card>
    </q-dialog>

    <template v-if="selPeer">
      <q-separator dark class="q-mt-sm" />
      <div class="text-caption q-mt-xs" style="opacity:0.7;font-weight:600">
        Peer {{ selectedIdx }}
      </div>
      <div class="row items-center no-wrap">
        <div class="col-4 text-caption">Enabled</div>
        <div class="col">
          <q-toggle :model-value="!!selPeer.enable" dense color="primary"
            @update:model-value="setField('enable', $event ? 1 : 0)" />
        </div>
      </div>
      <PeerField label="Host" :value="String(selPeer.host ?? '')" @change="setField('host', $event)" />
      <PeerField label="Port" :value="String(selPeer.port ?? '')" @change="setField('port', Number($event))" />
      <div class="row items-center no-wrap">
        <div class="col-4 text-caption">Mode</div>
        <q-select class="col" :model-value="String(selPeer.mode ?? 'gateway')" dense outlined
          :options="modeOptions" emit-value map-options
          @update:model-value="setField('mode', $event)" />
      </div>

      <q-expansion-item dense dense-toggle label="Advanced" header-class="text-caption ifac-adv" class="q-mt-xs">
        <div class="q-pl-sm q-gutter-y-sm q-pt-sm">
          <div class="text-caption" style="opacity:0.6">
            IFAC (Interface Access Codes): set a network name + passphrase to
            join an access-coded RNS network. Both must match the peer's, or
            traffic is dropped. Leave blank for an open interface.
          </div>
          <PeerField label="IFAC network" :value="String(selPeer.ifac_netname ?? '')"
            @change="setField('ifac_netname', $event)" />
          <div class="row items-center no-wrap">
            <div class="col-4 text-caption">IFAC passphrase</div>
            <q-input class="col" :model-value="ifacKeyInput" type="password" dense outlined
              debounce="600" placeholder="(write-only — set to change)"
              autocomplete="new-password" autocorrect="off" autocapitalize="off" spellcheck="false"
              @update:model-value="onIfacKeyInput" />
          </div>
        </div>
      </q-expansion-item>
    </template>

    <q-separator dark class="q-mt-md" />

    <div class="text-caption" style="opacity:0.7;font-weight:600">Incoming</div>
    <div class="text-caption" style="opacity:0.6">
      Accept inbound TCP connections — other Reticulum nodes dial this device on
      the listen port. Each accepted connection becomes its own RNS interface.
    </div>

    <SettingToggle label="Enabled" k="s.tcp.server_enable" />
    <SettingText   label="Listen port" k="s.tcp.server_port" />
    <SettingSelect label="Mode" k="s.tcp.server_mode" :options="modeOptions" />

    <q-expansion-item dense dense-toggle label="Advanced" header-class="text-caption" class="q-mt-xs">
      <div class="q-pl-sm q-gutter-y-sm q-pt-sm">
        <SettingText label="Max inbound" k="s.tcp.max_inbound" />
        <div class="text-caption" style="opacity:0.6">
          IFAC for accepted connections: a network name + passphrase that dialing
          peers must match, or their traffic is dropped.
        </div>
        <SettingText label="IFAC network" k="s.tcp.server_ifac_netname" />
        <div class="row items-center no-wrap">
          <div class="col-4 text-caption">IFAC passphrase</div>
          <q-input class="col" :model-value="serverIfacKey" type="password" dense outlined
            debounce="600" placeholder="(write-only — set to change)"
            autocomplete="new-password" autocorrect="off" autocapitalize="off" spellcheck="false"
            @update:model-value="setServerIfacKey" />
        </div>
      </div>
    </q-expansion-item>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useDeviceStore } from 'spangap-browser/stores/device'

const device = useDeviceStore()

interface Peer { enable?: number; host?: string; port?: number; mode?: string; ifac_netname?: string }

const modeOptions = [
  { label: 'Full', value: 'full' },
  { label: 'Gateway', value: 'gateway' },
  { label: 'Access point', value: 'access_point' },
  { label: 'Roaming', value: 'roaming' },
  { label: 'Boundary', value: 'boundary' },
]

const peers = computed<Peer[]>(() => {
  const arr = device.get('s.tcp.peers')
  if (!Array.isArray(arr)) return []
  return arr.map((p: any) => ({
    enable: Number(p?.enable ?? 0),
    host: String(p?.host ?? ''),
    port: Number(p?.port ?? 4965),
    mode: String(p?.mode ?? 'gateway'),
    ifac_netname: String(p?.ifac_netname ?? ''),
  }))
})

const selectedIdx = ref(-1)
const selPeer = computed(() => {
  const i = selectedIdx.value
  return (i >= 0 && i < peers.value.length) ? peers.value[i] : null
})

watch(peers, (n) => {
  if (selectedIdx.value >= n.length) selectedIdx.value = n.length - 1
}, { immediate: true })

function peerState(idx: number): string {
  return String(device.get(`tcp.peers.${idx}.state`) ?? 'idle')
}

// IFAC passphrase lives in secrets.* — it never syncs back to the browser, so
// this field is write-only. Clear it whenever the selected peer changes.
const ifacKeyInput = ref('')
watch(selectedIdx, () => { ifacKeyInput.value = '' })
function onIfacKeyInput(val: string | number | null) {
  ifacKeyInput.value = String(val ?? '')
  if (selectedIdx.value < 0) return
  device.set(`secrets.tcp.peers.${selectedIdx.value}.ifac_netkey`, ifacKeyInput.value)
  device.save()
}

// Inbound-server IFAC passphrase — also a secret, also write-only.
const serverIfacKey = ref('')
function setServerIfacKey(val: string | number | null) {
  serverIfacKey.value = String(val ?? '')
  device.set('secrets.tcp.server_ifac_netkey', serverIfacKey.value)
  device.save()
}

function writePeers(arr: any[]) {
  device.sendJson({ s: { tcp: { peers: arr } } })
  device.save()
}

function setField(field: keyof Peer, val: any) {
  if (selectedIdx.value < 0) return
  const arr = [...peers.value]
  arr[selectedIdx.value] = { ...arr[selectedIdx.value], [field]: val }
  writePeers(arr)
}

const showAddDialog = ref(false)
const newHost = ref('')
const newPort = ref(4965)

function openAddDialog() {
  newHost.value = ''
  newPort.value = 4965
  showAddDialog.value = true
}

/* Split a pasted "host:port" into the two fields. Greedy match before the
 * last `:digits` handles bracketed IPv6 like "[::1]:4965" too. */
function onHostInput(val: string | number | null) {
  const s = String(val ?? '')
  const m = s.match(/^(.+):(\d+)$/)
  if (m) {
    newHost.value = m[1]
    newPort.value = Number(m[2])
  } else {
    newHost.value = s
  }
}

function confirmAdd() {
  const host = newHost.value.trim()
  if (!host) return
  const arr = [...peers.value]
  arr.push({ enable: 1, host, port: Number(newPort.value) || 4965 })
  writePeers(arr)
  selectedIdx.value = arr.length - 1
  showAddDialog.value = false
}

function removePeer() {
  if (selectedIdx.value < 0) return
  const arr = [...peers.value]
  arr.splice(selectedIdx.value, 1)
  writePeers(arr)
  if (selectedIdx.value >= arr.length) selectedIdx.value = arr.length - 1
}

const dragIdx = ref(-1)
function onDragStart(idx: number, e: DragEvent) {
  dragIdx.value = idx
  e.dataTransfer!.effectAllowed = 'move'
}
function onDragOver(idx: number, e: DragEvent) {
  if (dragIdx.value < 0 || dragIdx.value === idx) return
  e.dataTransfer!.dropEffect = 'move'
}
function onDrop(targetIdx: number) {
  const fromIdx = dragIdx.value
  dragIdx.value = -1
  if (fromIdx < 0 || fromIdx === targetIdx) return
  const arr = [...peers.value]
  const [moved] = arr.splice(fromIdx, 1)
  arr.splice(targetIdx, 0, moved)
  writePeers(arr)
  selectedIdx.value = targetIdx
}

const PeerField = {
  props: { label: String, value: String },
  emits: ['change'],
  template: `<div class="row items-center no-wrap">
    <div class="col-4 text-caption">{{ label }}</div>
    <q-input class="col" :model-value="value" dense outlined debounce="500"
      autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"
      @update:model-value="$emit('change', $event)" />
  </div>`,
}
</script>

<style scoped>
.peer-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border-radius: 4px;
  cursor: pointer;
  user-select: none;
  color: rgba(255,255,255,0.7);
  transition: background 0.1s;
}
.peer-item:hover { background: rgba(255,255,255,0.06); }
.peer-item.selected { background: rgba(255,255,255,0.1); outline: 1px solid rgba(255,255,255,0.2); }
.peer-drag-handle { cursor: grab; opacity: 0.3; font-size: 14px; flex-shrink: 0; }
.peer-label { flex: 1; font-size: 14px; font-family: monospace; }
.peer-btn {
  font-size: 16px !important;
  min-width: 32px !important;
  padding: 2px 10px !important;
  background: rgba(255,255,255,0.08) !important;
  color: rgba(255,255,255,0.7) !important;
}
.peer-btn:hover { background: rgba(255,255,255,0.16) !important; }
</style>
