import { Router } from 'express';
import {
  getServiceStatus,
  controlService,
  streamLogs,
  stopLogStream,
  isAllowedService,
} from '../services/serviceManager.js';

const router = Router();

// GET /api/bff/services — list all services with status
router.get('/', async (_req, res) => {
  try {
    const services = await getServiceStatus();
    res.json(services);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/bff/services/:name/:action — start/stop/restart
router.post('/:name/:action', async (req, res) => {
  const { name, action } = req.params;
  if (!['start', 'stop', 'restart'].includes(action)) {
    res.status(400).json({ error: `Invalid action: ${action}` });
    return;
  }
  if (!isAllowedService(name)) {
    res.status(403).json({ error: `Service not allowed: ${name}` });
    return;
  }
  try {
    const result = await controlService(name, action as 'start' | 'stop' | 'restart');
    res.json({ message: result });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/bff/services/:name/logs — SSE log stream
router.get('/:name/logs', (req, res) => {
  const { name } = req.params;
  const tail = parseInt(req.query.tail as string) || 200;

  if (!isAllowedService(name)) {
    res.status(403).json({ error: `Service not allowed: ${name}` });
    return;
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  try {
    const { streamId, process: proc } = streamLogs(name, tail);

    // Send streamId so client can stop it later
    res.write(`data: ${JSON.stringify({ type: 'init', streamId })}\n\n`);

    proc.stdout?.on('data', (chunk: Buffer) => {
      res.write(`data: ${JSON.stringify({ type: 'log', text: chunk.toString() })}\n\n`);
    });

    proc.stderr?.on('data', (chunk: Buffer) => {
      res.write(`data: ${JSON.stringify({ type: 'log', text: chunk.toString() })}\n\n`);
    });

    proc.on('close', () => {
      res.write(`data: ${JSON.stringify({ type: 'end' })}\n\n`);
      res.end();
    });

    req.on('close', () => {
      stopLogStream(streamId);
    });
  } catch (e: any) {
    res.write(`data: ${JSON.stringify({ type: 'error', text: e.message })}\n\n`);
    res.end();
  }
});

// POST /api/bff/services/:name/logs/stop — stop a log stream
router.post('/:name/logs/stop', (req, res) => {
  const streamId = req.body?.streamId;
  if (!streamId) {
    res.status(400).json({ error: 'streamId required' });
    return;
  }
  const stopped = stopLogStream(streamId);
  res.json({ stopped });
});

export default router;
