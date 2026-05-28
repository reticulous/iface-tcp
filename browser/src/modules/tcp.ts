import { useMenuStore } from 'spangap-browser/stores/menu'
import TcpPanel from '../panels/TcpPanel.vue'

export function registerTcp() {
  useMenuStore().register('settings', 'Settings', 10, [
    {
      id: 'reticulum', label: 'Reticulum', type: 'submenu', order: 30,
      children: [
        {
          id: 'reticulum.transports', label: 'Transports', type: 'submenu', order: 20,
          children: [
            { id: 'reticulum.transports.tcp', label: 'TCP', type: 'panel', order: 10,
              component: TcpPanel },
          ],
        },
      ],
    },
  ])
}
