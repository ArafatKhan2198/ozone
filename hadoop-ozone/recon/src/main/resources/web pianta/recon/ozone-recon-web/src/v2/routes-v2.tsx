import { Pipelines } from './pages/pipelines/pipelines';
import { Volumes } from './pages/volumes/volumes';
import { Buckets } from './pages/buckets/buckets';
import { AIAssistant } from './pages/aiAssistant/AIAssistant';

export const routesV2: IRoute[] = [
  {
    path: '/pipelines',
    component: Pipelines,
    exact: true
  },
  {
    path: '/volumes',
    component: Volumes,
    exact: true
  },
  {
    path: '/buckets',
    component: Buckets,
    exact: true
  },
  {
    path: '/insights',
    component: Insights,
    exact: true
  },
  {
    path: '/AI-Assistant',
    component: AIAssistant,
    exact: true
  },
  {
    path: '*',
    component: NotFound
  }
];





