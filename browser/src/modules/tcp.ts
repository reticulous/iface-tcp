import { useMenuStore } from 'spangap-browser/stores/menu'
import TcpPanel from '../panels/TcpPanel.vue'

export function registerTcp() {
  useMenuStore().register('settings/reticulum/transports/tcp', 'TCP', { type: 'panel', component: TcpPanel })
}
