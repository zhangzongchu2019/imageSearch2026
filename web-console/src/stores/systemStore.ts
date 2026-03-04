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
      // silent — endpoint may 500 if degrade monitor not fully initialized
    }
  },

  fetchBreakers: async () => {
    try {
      const { data } = await adminApi.getBreakers();
      // Backend returns {name: {state, fail_count, ...}} object, convert to array
      let list: BreakerInfo[];
      if (Array.isArray(data)) {
        list = data;
      } else if (data && typeof data === 'object') {
        list = Object.entries(data).map(([key, val]: [string, any]) => ({
          name: val.name || key,
          state: (val.state || 'closed').toLowerCase().replace('closed', 'closed').replace('open', 'open').replace('half_open', 'half_open') as BreakerInfo['state'],
          fail_count: val.fail_count ?? 0,
          last_state_change: val.last_state_change || '',
        }));
      } else {
        list = [];
      }
      set({ breakers: list });
    } catch {
      // silent when disconnected
    }
  },

  fetchRateLimiter: async () => {
    try {
      const { data } = await adminApi.getRateLimiterStatus();
      // Backend returns {limiter_name: {rate_per_second, burst_limit, current_tokens, fill_percent}}
      // Convert to our expected format
      let status: RateLimiterStatus | null = null;
      if (data && typeof data === 'object') {
        if ('tokens_remaining' in data) {
          // Already in expected format
          status = data as RateLimiterStatus;
        } else {
          // Extract first limiter entry
          const first = Object.values(data)[0] as any;
          if (first) {
            status = {
              tokens_remaining: first.current_tokens ?? 0,
              max_tokens: first.burst_limit ?? 0,
              refill_rate: first.rate_per_second ?? 0,
            };
          }
        }
      }
      set({ rateLimiter: status });
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
