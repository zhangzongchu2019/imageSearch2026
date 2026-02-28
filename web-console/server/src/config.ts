export const config = {
  port: parseInt(process.env.BFF_PORT || '3001'),
  searchServiceUrl: process.env.SEARCH_SERVICE_URL || 'http://localhost:8080',
  writeServiceUrl: process.env.WRITE_SERVICE_URL || 'http://localhost:8081',
  cronServiceUrl: process.env.CRON_SERVICE_URL || 'http://localhost:8082',
  uploadDir: process.env.UPLOAD_DIR || '/tmp/imgsrch-uploads',
  batchMaxSize: parseInt(process.env.BATCH_MAX_SIZE || '128'),
  batchConcurrency: parseInt(process.env.BATCH_CONCURRENCY || '8'),
  uploadMaxAge: 3600_000, // 1 hour in ms
  projectRoot: process.env.PROJECT_ROOT || '/home/zzc/IdeaProjects/imageSearch2026',
};
