import { useEffect } from 'react';
import { useHistoryStore } from '../stores/historyStore';

export function useHistory() {
  const store = useHistoryStore();

  useEffect(() => {
    store.loadHistory();
  }, []);

  return store;
}
