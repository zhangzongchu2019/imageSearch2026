import express from 'express';
import cors from 'cors';
import path from 'path';
import { config } from './config.js';
import { injectApiKey } from './middleware/auth.js';
import uploadRoutes from './routes/upload.js';
import proxyRoutes from './routes/proxy.js';
import batchRoutes from './routes/batch.js';
import testRoutes from './routes/tests.js';
import schedulerRoutes from './routes/scheduler.js';

const app = express();

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(injectApiKey);

// Serve uploaded files
app.use('/uploads', express.static(config.uploadDir));

// BFF routes
app.use('/api/bff', uploadRoutes);
app.use('/api/bff', proxyRoutes);
app.use('/api/bff/batch', batchRoutes);
app.use('/api/bff/tests', testRoutes);
app.use('/api/bff/scheduler', schedulerRoutes);

// Health check
app.get('/healthz', (_req, res) => res.json({ status: 'ok' }));

app.listen(config.port, () => {
  console.log(`BFF server running on http://localhost:${config.port}`);
  console.log(`  Search service: ${config.searchServiceUrl}`);
  console.log(`  Write service:  ${config.writeServiceUrl}`);
  console.log(`  Cron service:   ${config.cronServiceUrl}`);
  console.log(`  Upload dir:     ${config.uploadDir}`);
});
