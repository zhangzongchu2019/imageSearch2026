import { Router } from 'express';
import multer from 'multer';
import { v4 as uuid } from 'uuid';
import path from 'path';
import fs from 'fs';
import readline from 'readline';
import { config } from '../config.js';
import { orchestrateBatchImport } from '../services/batchOrchestrator.js';
import { searchClient } from '../services/searchClient.js';
import { writeClient } from '../services/writeClient.js';

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
    url: `${config.uploadUrlBase}/uploads/${f.filename}`,
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

// File import — read URL list and call write-service API for each
const txtUpload = multer({ storage, limits: { fileSize: 1024 * 1024 * 1024 } }); // 1GB

router.post('/file-import', txtUpload.single('file'), async (req, res) => {
  // Disable timeouts for long-running import
  req.socket.setTimeout(0);
  req.socket.setKeepAlive(true);

  const file = req.file;
  if (!file) {
    res.status(400).json({ message: 'No file uploaded' });
    return;
  }

  const params = JSON.parse(req.body.params || '{}');
  const startLine = Math.max(1, params.start || 1);          // 1-based
  const endLine = params.end || 10000000;
  const concurrency = Math.min(64, Math.max(1, params.concurrency || config.batchConcurrency));
  const maxRetries = Math.min(10, Math.max(0, params.retries ?? 2));

  // SSE headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const sendSSE = (data: object) => {
    try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {}
  };

  let aborted = false;
  req.on('close', () => { aborted = true; });

  try {
    // 1) Read URLs from file — stream, then slice [startLine, endLine]
    sendSSE({ stage: 'collect', completed: 0, total: 0, message: '正在读取 URL 列表...' });
    const allUrls: string[] = await new Promise((resolve, reject) => {
      const lines: string[] = [];
      const rl = readline.createInterface({ input: fs.createReadStream(file.path, 'utf-8'), crlfDelay: Infinity });
      rl.on('line', (line) => {
        const trimmed = line.trim();
        if (trimmed) lines.push(trimmed);
      });
      rl.on('close', () => resolve(lines));
      rl.on('error', reject);
    });
    // Slice by line range (1-based inclusive)
    const urls = allUrls.slice(startLine - 1, endLine);
    const total = urls.length;

    if (total === 0) {
      sendSSE({ stage: 'error', completed: 0, total: 0, message: '文件中没有有效的 URL' });
      res.write('event: done\ndata: {}\n\n');
      res.end();
      fs.unlink(file.path, () => {});
      return;
    }

    sendSSE({ stage: 'collect', completed: 0, total, message: `文件共 ${allUrls.length} 行，选取第 ${startLine}~${startLine + total - 1} 行，共 ${total} 条 URL` });
    sendSSE({ type: 'log', message: `将处理 ${total} 条 URL（并行 ${concurrency}，重试 ${maxRetries} 次）` });

    // 2) Process URLs in batches via write-service API
    let completed = 0;
    let succeeded = 0;
    let failed = 0;
    const apiKey = req.headers['x-api-key'] as string | undefined;

    // Save checkpoint for resume
    const checkpointFile = path.join(config.uploadDir, 'batch_import_checkpoint.json');
    const saveCheckpoint = () => {
      try {
        fs.writeFileSync(checkpointFile, JSON.stringify({
          exists: true,
          stage: 'milvus',
          done: completed,
          count: total,
          succeeded,
          failed,
          url_file: file.path,
          skip_kafka: params.skip_kafka ?? true,
        }));
      } catch {}
    };

    // Helper: call write-service with retries
    const importOne = async (url: string): Promise<{ success: boolean; image_id?: string; error?: string }> => {
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          const resp = await writeClient.post(
            '/api/v1/image/update',
            {
              uri: url,
              merchant_id: `auto_${Date.now()}`,
              category_l1: '服装',
              product_id: `file_import_${completed}`,
            },
            {
              headers: apiKey ? { 'X-API-Key': apiKey } : {},
              timeout: 30000,
            },
          );
          return { success: true, image_id: resp.data.image_id };
        } catch (e: any) {
          if (attempt < maxRetries) {
            await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
            continue;
          }
          const msg = e.response?.data?.detail?.error?.message || e.response?.data?.message || e.message;
          return { success: false, error: msg };
        }
      }
      return { success: false, error: 'unreachable' };
    };

    for (let i = 0; i < total && !aborted; i += concurrency) {
      const batch = urls.slice(i, i + concurrency);
      const promises = batch.map(async (url) => {
        const result = await importOne(url);
        if (result.success) {
          succeeded++;
        } else {
          failed++;
          sendSSE({ type: 'log', message: `[失败] ${url}: ${result.error}` });
        }
        completed++;
        return result;
      });

      await Promise.allSettled(promises);

      // Send progress
      const stage = completed >= total ? 'done' : 'milvus';
      sendSSE({
        stage,
        completed,
        total,
        message: `已处理 ${completed}/${total} (成功 ${succeeded}, 失败 ${failed})`,
      });

      // Save checkpoint every batch
      if (completed % (concurrency * 5) === 0) {
        saveCheckpoint();
      }
    }

    // Final
    if (aborted) {
      saveCheckpoint();
      sendSSE({ type: 'log', message: '导入被中断，已保存断点' });
    } else {
      sendSSE({ stage: 'done', completed, total, message: `导入完成: 成功 ${succeeded}, 失败 ${failed}` });
      sendSSE({ type: 'log', message: `导入完成！共 ${total} 条，成功 ${succeeded}，失败 ${failed}` });
      // Clean up checkpoint on success
      try { fs.unlinkSync(checkpointFile); } catch {}
    }
  } catch (e: any) {
    sendSSE({ stage: 'error', completed: 0, total: 0, message: `导入失败: ${e.message}` });
    sendSSE({ type: 'log', message: `错误: ${e.stack || e.message}` });
  } finally {
    fs.unlink(file.path, () => {});
    res.write('event: done\ndata: {}\n\n');
    res.end();
  }
});

// Resume import from checkpoint
router.post('/file-import/resume', async (_req, res) => {
  const checkpointPath = path.join(config.uploadDir, 'batch_import_checkpoint.json');
  if (!fs.existsSync(checkpointPath)) {
    res.status(404).json({ message: 'No checkpoint found' });
    return;
  }

  _req.socket.setTimeout(0);
  _req.socket.setKeepAlive(true);

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const sendSSE = (data: object) => {
    try { res.write(`data: ${JSON.stringify(data)}\n\n`); } catch {}
  };

  try {
    const checkpoint = JSON.parse(fs.readFileSync(checkpointPath, 'utf-8'));
    const urlFile = checkpoint.url_file;

    if (!urlFile || !fs.existsSync(urlFile)) {
      sendSSE({ stage: 'error', completed: 0, total: 0, message: 'URL 文件已丢失，无法恢复' });
      res.write('event: done\ndata: {}\n\n');
      res.end();
      return;
    }

    const maxCount = checkpoint.count || 10000;
    const startFrom = checkpoint.done || 0;
    const urls: string[] = await new Promise((resolve, reject) => {
      const lines: string[] = [];
      const rl = readline.createInterface({ input: fs.createReadStream(urlFile, 'utf-8'), crlfDelay: Infinity });
      rl.on('line', (line) => {
        const trimmed = line.trim();
        if (trimmed && lines.length < maxCount) lines.push(trimmed);
      });
      rl.on('close', () => resolve(lines));
      rl.on('error', reject);
    });
    const remaining = urls.slice(startFrom);
    const total = urls.length;

    sendSSE({ stage: 'milvus', completed: startFrom, total, message: `从第 ${startFrom + 1} 条恢复导入...` });

    let completed = startFrom;
    let succeeded = checkpoint.succeeded || 0;
    let failed = checkpoint.failed || 0;
    let aborted = false;
    _req.on('close', () => { aborted = true; });

    const concurrency = config.batchConcurrency;
    const apiKey = _req.headers['x-api-key'] as string | undefined;

    for (let i = 0; i < remaining.length && !aborted; i += concurrency) {
      const batch = remaining.slice(i, i + concurrency);
      const promises = batch.map(async (url) => {
        try {
          await writeClient.post('/api/v1/image/update', {
            uri: url,
            merchant_id: `auto_${Date.now()}`,
            category_l1: '服装',
            product_id: `file_import_${completed}`,
          }, { timeout: 30000 });
          succeeded++;
        } catch (e: any) {
          failed++;
          const msg = e.response?.data?.detail?.error?.message || e.message;
          sendSSE({ type: 'log', message: `[失败] ${url}: ${msg}` });
        } finally {
          completed++;
        }
      });

      await Promise.allSettled(promises);
      sendSSE({ stage: completed >= total ? 'done' : 'milvus', completed, total, message: `已处理 ${completed}/${total} (成功 ${succeeded}, 失败 ${failed})` });
    }

    sendSSE({ stage: 'done', completed, total, message: `恢复导入完成: 成功 ${succeeded}, 失败 ${failed}` });
    try { fs.unlinkSync(checkpointPath); } catch {}
  } catch (e: any) {
    sendSSE({ stage: 'error', completed: 0, total: 0, message: `恢复失败: ${e.message}` });
  } finally {
    res.write('event: done\ndata: {}\n\n');
    res.end();
  }
});

// Check if a resume checkpoint exists
router.get('/file-import/checkpoint', (_req, res) => {
  const checkpointPath = path.join(config.uploadDir, 'batch_import_checkpoint.json');
  if (!fs.existsSync(checkpointPath)) {
    res.json({ exists: false });
    return;
  }
  try {
    const data = JSON.parse(fs.readFileSync(checkpointPath, 'utf-8'));
    res.json({ exists: true, ...data });
  } catch {
    res.json({ exists: false });
  }
});

export default router;
