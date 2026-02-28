import { Router } from 'express';
import multer from 'multer';
import { v4 as uuid } from 'uuid';
import path from 'path';
import fs from 'fs';
import { config } from '../config.js';
import { orchestrateBatchImport } from '../services/batchOrchestrator.js';
import { searchClient } from '../services/searchClient.js';

const router = Router();

const storage = multer.diskStorage({
  destination: config.uploadDir,
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname) || '.jpg';
    cb(null, `${uuid()}${ext}`);
  },
});

const upload = multer({ storage, limits: { fileSize: 10 * 1024 * 1024 } });

// Batch import with SSE progress
router.post('/import', upload.array('files', 128), async (req, res) => {
  const files = req.files as Express.Multer.File[];
  const meta = JSON.parse(req.body.metadata || '{}');

  const items = (files || []).map((f) => ({
    url: `http://localhost:${config.port}/uploads/${f.filename}`,
    merchant_id: meta.merchant_id || '',
    category_l1: meta.category_l1 || '',
    product_id: meta.product_id || '',
    tags: meta.tags,
  }));

  // Also handle URL list
  if (req.body.urls) {
    const urls: string[] = JSON.parse(req.body.urls);
    for (const url of urls) {
      items.push({
        url,
        merchant_id: meta.merchant_id || '',
        category_l1: meta.category_l1 || '',
        product_id: meta.product_id || '',
        tags: meta.tags,
      });
    }
  }

  const apiKey = req.headers['x-api-key'] as string | undefined;
  await orchestrateBatchImport(items, apiKey, res);
});

// Batch search with SSE progress
router.post('/search', upload.array('files', 128), async (req, res) => {
  const files = req.files as Express.Multer.File[];
  if (!files?.length) {
    res.status(400).json({ message: 'No files uploaded' });
    return;
  }

  const params = JSON.parse(req.body.params || '{}');
  const apiKey = req.headers['x-api-key'] as string | undefined;

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const total = files.length;
  let completed = 0;
  const results: any[] = [];

  for (let i = 0; i < total; i += config.batchConcurrency) {
    const batch = files.slice(i, i + config.batchConcurrency);
    const promises = batch.map(async (file, j) => {
      const index = i + j;
      try {
        const imageData = fs.readFileSync(file.path);
        const base64 = imageData.toString('base64');
        const resp = await searchClient.post(
          '/api/v1/image/search',
          { query_image: base64, ...params },
          { headers: apiKey ? { 'X-API-Key': apiKey } : {} },
        );
        results.push({ index, success: true, data: resp.data });
      } catch (e: any) {
        results.push({ index, success: false, error: e.message });
      }
      completed++;
      res.write(`data: ${JSON.stringify({ completed, total, results })}\n\n`);
    });
    await Promise.allSettled(promises);
  }

  res.write('event: done\ndata: {}\n\n');
  res.end();
});

export default router;
