import React, { lazy, Suspense } from 'react';
import type { RouteObject } from 'react-router-dom';
import { Spin } from 'antd';

const Loading = () => (
  <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '50vh' }}>
    <Spin size="large" />
  </div>
);

const wrap = (Component: React.LazyExoticComponent<() => React.JSX.Element>) => (
  <Suspense fallback={<Loading />}>
    <Component />
  </Suspense>
);

const Dashboard = lazy(() => import('./pages/Dashboard'));
const ImageImport = lazy(() => import('./pages/ImageImport'));
const ImageSearch = lazy(() => import('./pages/ImageSearch'));
const History = lazy(() => import('./pages/History'));
const Degrade = lazy(() => import('./pages/Admin/Degrade'));
const Breakers = lazy(() => import('./pages/Admin/Breakers'));
const Config = lazy(() => import('./pages/Admin/Config'));
const Tests = lazy(() => import('./pages/Admin/Tests'));
const Scheduler = lazy(() => import('./pages/Admin/Scheduler'));

export const routes: RouteObject[] = [
  { path: '/', element: wrap(Dashboard) },
  { path: '/import', element: wrap(ImageImport) },
  { path: '/search', element: wrap(ImageSearch) },
  { path: '/history', element: wrap(History) },
  { path: '/admin/degrade', element: wrap(Degrade) },
  { path: '/admin/breakers', element: wrap(Breakers) },
  { path: '/admin/config', element: wrap(Config) },
  { path: '/admin/tests', element: wrap(Tests) },
  { path: '/admin/scheduler', element: wrap(Scheduler) },
];
