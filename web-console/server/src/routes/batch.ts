import { Router } from 'express';
import multer from 'multer';
import { v4 as uuid } from 'uuid';
import path from 'path';
import fs from 'fs';
import { spawn } from 'child_process';
import readline from 'readline';
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

// File import — spawn Python script with SSE progress
const txtUpload = multer({ storage, limits: { fileSize: 1024 * 1024 * 1024 } }); // 1GB

router.post('/file-import', txtUpload.single('file'), (req, res) => {
  // Disable timeouts for long-running import
  req.socket.setTimeout(0);
  req.socket.setKeepAlive(true);

  const file = req.file;
  if (!file) {
    res.status(400).json({ message: 'No file uploaded' });
    return;
  }

  const params = JSON.parse(req.body.params || '{}');
  const count = String(params.count || 10000);
  const skipKafka = params.skip_kafka ?? true;

  // SSE headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const scriptPath = path.resolve(config.projectRoot, 'scripts', 'batch_import_clothing.py');
  const args = [
    scriptPath,
    '--url-file', file.path,
    '--count', count,
  ];
  if (skipKafka) {
    args.push('--skip-kafka');
  }

  const checkpointFile = path.join(config.uploadDir, 'batch_import_checkpoint.json');
  const child = spawn('python3', args, {
    env: { ...process.env, CHECKPOINT_FILE: checkpointFile },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const rl = readline.createInterface({ input: child.stdout });
  const rlErr = readline.createInterface({ input: child.stderr });

  const sendSSE = (data: object) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  rl.on('line', (line) => {
    if (line.startsWith('##PROGRESS##')) {
      try {
        const json = JSON.parse(line.slice('##PROGRESS##'.length));
        sendSSE(json);
      } catch {
        sendSSE({ type: 'log', message: line });
      }
    } else {
      sendSSE({ type: 'log', message: line });
    }
  });

  rlErr.on('line', (line) => {
    sendSSE({ type: 'log', message: line });
  });

  child.on('close', (code) => {
    if (code === 0) {
      sendSSE({ stage: 'done', completed: 0, total: 0, message: 'Import completed successfully' });
    } else {
      sendSSE({ stage: 'error', completed: 0, total: 0, message: `Process exited with code ${code}` });
    }
    // Always clean up uploaded file (checkpoint handles resume, not the raw file)
    fs.unlink(file.path, () => {});
    res.write('event: done\ndata: {}\n\n');
    res.end();
  });

  child.on('error', (err) => {
    sendSSE({ stage: 'error', completed: 0, total: 0, message: err.message });
    res.write('event: done\ndata: {}\n\n');
    res.end();
  });

  req.on('close', () => {
    child.kill('SIGTERM');
  });
});

// Resume import from checkpoint
router.post('/file-import/resume', (_req, res) => {
  const checkpointPath = path.join(config.uploadDir, 'batch_import_checkpoint.json');
  if (!fs.existsSync(checkpointPath)) {
    res.status(404).json({ message: 'No checkpoint found' });
    return;
  }

  // Disable timeouts
  _req.socket.setTimeout(0);
  _req.socket.setKeepAlive(true);

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const checkpoint = JSON.parse(fs.readFileSync(checkpointPath, 'utf-8'));

  const scriptPath = path.resolve(config.projectRoot, 'scripts', 'batch_import_clothing.py');
  const args = [
    scriptPath,
    '--resume',
    '--count', String(checkpoint.count || 10000),
    '--download-dir', checkpoint.download_dir || '/tmp/clothing_images',
  ];
  if (checkpoint.url_file) {
    args.push('--url-file', checkpoint.url_file);
  }
  if (checkpoint.model_path) {
    args.push('--model-path', checkpoint.model_path);
  }
  if (checkpoint.skip_kafka) {
    args.push('--skip-kafka');
  }

  const child = spawn('python3', args, {
    env: { ...process.env, CHECKPOINT_FILE: checkpointPath },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const rl = readline.createInterface({ input: child.stdout });
  const rlErr = readline.createInterface({ input: child.stderr });

  const sendSSE = (data: object) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  rl.on('line', (line) => {
    if (line.startsWith('##PROGRESS##')) {
      try {
        const json = JSON.parse(line.slice('##PROGRESS##'.length));
        sendSSE(json);
      } catch {
        sendSSE({ type: 'log', message: line });
      }
    } else {
      sendSSE({ type: 'log', message: line });
    }
  });

  rlErr.on('line', (line) => {
    sendSSE({ type: 'log', message: line });
  });

  child.on('close', (code) => {
    if (code === 0) {
      sendSSE({ stage: 'done', completed: 0, total: 0, message: 'Import resumed and completed' });
    } else {
      sendSSE({ stage: 'error', completed: 0, total: 0, message: `Process exited with code ${code}` });
    }
    res.write('event: done\ndata: {}\n\n');
    res.end();
  });

  child.on('error', (err) => {
    sendSSE({ stage: 'error', completed: 0, total: 0, message: err.message });
    res.write('event: done\ndata: {}\n\n');
    res.end();
  });

  _req.on('close', () => {
    child.kill('SIGTERM');
  });
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
