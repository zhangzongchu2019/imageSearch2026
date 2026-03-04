import { useRoutes } from 'react-router-dom';
import AppLayout from './components/Layout/AppLayout';
import ErrorBoundary from './components/ErrorBoundary';
import { routes } from './routes';

export default function App() {
  const element = useRoutes(routes);
  return (
    <ErrorBoundary>
      <AppLayout>{element}</AppLayout>
    </ErrorBoundary>
  );
}
