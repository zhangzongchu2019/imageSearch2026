import client from './client';
import type { UploadResponse, SchedulerJob, JobHistory, TestRun } from './types';

export const bffApi = {
  uploadFile(file: File) {
    const form = new FormData();
    form.append('file', file);
    return client.post<UploadResponse>('/api/bff/upload', form);
  },

  importImage(data: { url: string; merchant_id: string; category_l1: string; product_id: string; tags?: string[] }) {
    return client.post('/api/bff/import', data);
  },

  // SSE endpoints — use EventSource directly
  batchImportUrl: '/api/bff/batch/import',
  batchSearchUrl: '/api/bff/batch/search',

  // Scheduler
  getJobs() {
    return client.get<SchedulerJob[]>('/api/bff/scheduler/jobs');
  },
  triggerJob(name: string) {
    return client.post(`/api/bff/scheduler/jobs/${name}/trigger`);
  },
  getJobHistory(name: string) {
    return client.get<JobHistory[]>(`/api/bff/scheduler/jobs/${name}/history`);
  },

  // Tests
  runTests(service: string, type: 'unit' | 'integration') {
    return client.post<{ runId: string }>('/api/bff/tests/run', { service, type });
  },
  // SSE: GET /api/bff/tests/stream/{runId}
};
