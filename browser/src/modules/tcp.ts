import { useMenuStore } from 'spangap-browser/stores/menu'
import TcpPanel from '../panels/TcpPanel.vue'

export function registerTcp() {
  useMenuStore().register('settings', 'Settings', [
    {
      id: 'reticulum', label: 'Reticulum', type: 'submenu',
      children: [
        {
          id: 'reticulum.transports', label: 'Transports', type: 'submenu',
          children: [
            { id: 'reticulum.transports.tcp', label: 'TCP', type: 'panel',
              component: TcpPanel },
          ],
        },
      ],
    },
  ])
}
