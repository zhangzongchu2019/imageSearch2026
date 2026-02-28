import client from './client';
import type {
  SystemStatus,
  DegradeStatus,
  BreakerInfo,
  RateLimiterStatus,
  ConfigAuditEntry,
  DegradeState,
  BreakerState,
} from './types';

export const adminApi = {
  getSystemStatus() {
    return client.get<SystemStatus>('/api/v1/system/status');
  },
  getDegradeStatus() {
    return client.get<DegradeStatus>('/admin/degrade/status');
  },
  overrideDegrade(state: DegradeState, reason: string) {
    return client.post('/admin/degrade/override', { state, reason });
  },
  releaseDegrade() {
    return client.post('/admin/degrade/release');
  },
  getBreakers() {
    return client.get<BreakerInfo[]>('/admin/breakers');
  },
  forceBreaker(name: string, state: BreakerState) {
    return client.post(`/admin/breakers/${name}/force`, { state });
  },
  getRateLimiterStatus() {
    return client.get<RateLimiterStatus>('/admin/rate_limiter/status');
  },
  reloadConfig() {
    return client.post('/admin/config/reload');
  },
  getConfigAudit() {
    return client.get<ConfigAuditEntry[]>('/admin/config/audit');
  },
};
