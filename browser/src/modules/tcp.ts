import { useMenuStore } from 'spangap-browser/stores/menu'
import TcpPanel from '../panels/TcpPanel.vue'

export function registerTcp() {
  const menu = useMenuStore()
  menu.setMenu('settings/mesh/interfaces', { label: 'RNS Interfaces', placement: 2 })
  menu.register('settings/mesh/interfaces/tcp', 'TCP', { type: 'panel', component: TcpPanel })
}
