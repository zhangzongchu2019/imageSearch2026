// Mapped from Pydantic models in search-service/app/model/schemas.py

export type DataScope = 'ALL' | 'ROLLING' | 'EVERGREEN';
export type TimeRange = 'HOT_ONLY' | 'ALL' | 'HOT_PLUS_EVERGREEN';
export type Confidence = 'HIGH' | 'MEDIUM' | 'LOW';
export type Strategy = 'FAST_PATH' | 'CASCADE_PATH';
export type DegradeState = 'S0' | 'S1' | 'S2' | 'S3' | 'S4';
export type BreakerState = 'closed' | 'open' | 'half_open';
export type EventType = 'impression' | 'click' | 'inquiry' | 'order' | 'negative_feedback';

// --- Request Types ---

export interface SearchRequest {
  query_image: string; // base64
  merchant_scope?: string[];
  merchant_scope_id?: string;
  top_k?: number;
  data_scope?: DataScope;
  time_range?: TimeRange;
}

export interface UpdateImageRequest {
  uri: string;
  merchant_id: string;
  category_l1: string;
  product_id: string;
  tags?: string[];
}

// --- Response Types ---

export interface SearchResultItem {
  image_id: string;
  score: number;
  product_id: string;
  position: number;
  is_evergreen: boolean;
  category_l1: string;
  tags: string[];
}

export interface SearchMeta {
  request_id: string;
  total_results: number;
  strategy: Strategy;
  confidence: Confidence;
  degraded: boolean;
  degrade_state: DegradeState;
  degrade_reason: string | null;
  search_scope_desc: string;
  latency_ms: number;
  zone_hit: string;
  feature_ms: number;
  ann_hot_ms: number;
  ann_non_hot_ms: number;
  tag_recall_ms: number;
  filter_ms: number;
  refine_ms: number;
}

export interface SearchResponse {
  results: SearchResultItem[];
  meta: SearchMeta;
}

export interface UpdateImageResponse {
  status: string;
  image_id: string;
  is_new: boolean;
}

export interface ErrorDetail {
  code: string;
  message: string;
  request_id: string;
  timestamp: string;
}

export interface ErrorResponse {
  error: ErrorDetail;
}

// --- Admin Types ---

export interface SystemStatus {
  status: string;
  degrade_state: DegradeState;
  epoch: number;
  uptime_seconds: number;
  version: string;
}

export interface DegradeStatus {
  current_state: DegradeState;
  epoch: number;
  window_metrics: {
    p99_ms: number;
    error_rate: number;
    timeout_count: number;
  };
  manual_override: boolean;
  override_reason?: string;
  redis_connected: boolean;
}

export interface BreakerInfo {
  name: string;
  state: BreakerState;
  fail_count: number;
  last_state_change: string;
}

export interface RateLimiterStatus {
  tokens_remaining: number;
  max_tokens: number;
  refill_rate: number;
}

export interface ConfigAuditEntry {
  version: number;
  timestamp: string;
  key: string;
  old_value: string;
  new_value: string;
}

// --- BFF Types ---

export interface UploadResponse {
  url: string;
  filename: string;
}

export interface BatchProgress {
  completed: number;
  total: number;
  succeeded: number;
  failed: number;
  results: BatchItemResult[];
}

export interface BatchItemResult {
  index: number;
  success: boolean;
  image_id?: string;
  is_new?: boolean;
  error?: string;
}

// --- Scheduler Types ---

export interface SchedulerJob {
  name: string;
  cron: string;
  description: string;
  next_run: string;
  last_run?: string;
  last_status?: 'success' | 'failed';
}

export interface JobHistory {
  timestamp: string;
  status: 'success' | 'failed';
  duration_ms: number;
  error?: string;
}

// --- Test Runner Types ---

export interface TestRun {
  runId: string;
  service: string;
  type: 'unit' | 'integration';
  status: 'running' | 'completed' | 'failed';
  passed: number;
  failed: number;
  errors: number;
  duration_ms: number;
  output: string;
}
