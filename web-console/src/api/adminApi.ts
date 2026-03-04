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
    return client.get<DegradeStatus>('/api/v1/admin/degrade/status');
  },
  overrideDegrade(state: DegradeState, reason: string) {
    return client.post('/api/v1/admin/degrade/override', { state, reason });
  },
  releaseDegrade() {
    return client.post('/api/v1/admin/degrade/release');
  },
  getBreakers() {
    return client.get<BreakerInfo[]>('/api/v1/admin/breakers');
  },
  forceBreaker(name: string, state: BreakerState) {
    return client.post(`/api/v1/admin/breakers/${name}/force`, { state });
  },
  getRateLimiterStatus() {
    return client.get<RateLimiterStatus>('/api/v1/admin/rate_limiter/status');
  },
  reloadConfig() {
    return client.post('/api/v1/admin/config/reload');
  },
  getConfigAudit() {
    return client.get<ConfigAuditEntry[]>('/api/v1/admin/config/audit');
  },
};
