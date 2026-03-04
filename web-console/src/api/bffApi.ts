import client from './client';
import type { UploadResponse, SchedulerJob, JobHistory, TestRun, ServiceInfo, MilvusPartitionInfo, MilvusDataResponse } from './types';

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
  fileImportUrl: '/api/bff/batch/file-import',
  fileImportResumeUrl: '/api/bff/batch/file-import/resume',

  getImportCheckpoint() {
    return client.get<{ exists: boolean; stage?: string; done?: number; count?: number }>('/api/bff/batch/file-import/checkpoint');
  },

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

  // Services
  getServices() {
    return client.get<ServiceInfo[]>('/api/bff/services');
  },
  controlService(name: string, action: 'start' | 'stop' | 'restart') {
    return client.post<{ message: string }>(`/api/bff/services/${name}/${action}`);
  },
  stopLogStream(name: string, streamId: string) {
    return client.post(`/api/bff/services/${name}/logs/stop`, { streamId });
  },

  // Milvus Data Browser
  getMilvusPartitions() {
    return client.get<MilvusPartitionInfo[]>('/api/bff/milvus/partitions');
  },
  getMilvusData(partition: string, offset: number, limit: number) {
    return client.get<MilvusDataResponse>('/api/bff/milvus/data', {
      params: { partition, offset, limit },
    });
  },

  // Metrics
  getPrometheusQps() {
    return client.get<{ qps: number }>('/api/bff/metrics/qps');
  },
};
