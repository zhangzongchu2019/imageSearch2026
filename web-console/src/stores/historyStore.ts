import { create } from 'zustand';
import { db, type HistoryRecord } from '../utils/db';

interface HistoryState {
  imports: HistoryRecord[];
  searches: HistoryRecord[];
  loading: boolean;
  loadHistory: () => Promise<void>;
  addRecord: (record: Omit<HistoryRecord, 'id' | 'timestamp'>) => Promise<void>;
  clearAll: () => Promise<void>;
}

export const useHistoryStore = create<HistoryState>((set) => ({
  imports: [],
  searches: [],
  loading: false,

  loadHistory: async () => {
    set({ loading: true });
    const imports = await db.history.where('type').equals('import').reverse().sortBy('timestamp');
    const searches = await db.history.where('type').equals('search').reverse().sortBy('timestamp');
    set({ imports, searches, loading: false });
  },

  addRecord: async (record) => {
    await db.history.add({ ...record, timestamp: Date.now() });
    // Trim to 500 records
    const count = await db.history.count();
    if (count > 500) {
      const oldest = await db.history.orderBy('timestamp').limit(count - 500).primaryKeys();
      await db.history.bulkDelete(oldest);
    }
  },

  clearAll: async () => {
    await db.history.clear();
    set({ imports: [], searches: [] });
  },
}));
