import { create } from 'zustand';
import type { SystemStatus, DegradeStatus, BreakerInfo, RateLimiterStatus } from '../api/types';
import { adminApi } from '../api/adminApi';

interface SystemState {
  status: SystemStatus | null;
  degradeStatus: DegradeStatus | null;
  breakers: BreakerInfo[];
  rateLimiter: RateLimiterStatus | null;
  loading: boolean;
  connected: boolean;
  error: string | null;
  fetchStatus: () => Promise<void>;
  fetchDegradeStatus: () => Promise<void>;
  fetchBreakers: () => Promise<void>;
  fetchRateLimiter: () => Promise<void>;
  fetchAll: () => Promise<void>;
}

export const useSystemStore = create<SystemState>((set) => ({
  status: null,
  degradeStatus: null,
  breakers: [],
  rateLimiter: null,
  loading: false,
  connected: false,
  error: null,

  fetchStatus: async () => {
    try {
      const { data } = await adminApi.getSystemStatus();
      set({ status: data, connected: true, error: null });
    } catch (e: any) {
      set({ connected: false, error: e.message });
    }
  },

  fetchDegradeStatus: async () => {
    try {
      const { data } = await adminApi.getDegradeStatus();
      set({ degradeStatus: data });
    } catch {
      // silent when disconnected
    }
  },

  fetchBreakers: async () => {
    try {
      const { data } = await adminApi.getBreakers();
      set({ breakers: data });
    } catch {
      // silent when disconnected
    }
  },

  fetchRateLimiter: async () => {
    try {
      const { data } = await adminApi.getRateLimiterStatus();
      set({ rateLimiter: data });
    } catch {
      // silent when disconnected
    }
  },

  fetchAll: async () => {
    set({ loading: true });
    const store = useSystemStore.getState();
    await Promise.allSettled([
      store.fetchStatus(),
      store.fetchDegradeStatus(),
      store.fetchBreakers(),
      store.fetchRateLimiter(),
    ]);
    set({ loading: false });
  },
}));
