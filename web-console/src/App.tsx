import { useRoutes } from 'react-router-dom';
import AppLayout from './components/Layout/AppLayout';
import { routes } from './routes';

export default function App() {
  const element = useRoutes(routes);
  return <AppLayout>{element}</AppLayout>;
}
