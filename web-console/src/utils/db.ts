import Dexie, { type EntityTable } from 'dexie';

export interface HistoryRecord {
  id?: number;
  type: 'import' | 'search';
  timestamp: number;
  thumbnail?: string; // base64 data URL
  params: Record<string, unknown>;
  result: Record<string, unknown>;
}

const db = new Dexie('imgsrch-history') as Dexie & {
  history: EntityTable<HistoryRecord, 'id'>;
};

db.version(1).stores({
  history: '++id, type, timestamp',
});

export { db };
