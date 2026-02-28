import { Router } from 'express';
import { startTestRun, getTestRun } from '../services/testRunner.js';

const router = Router();

// Start a test run
router.post('/run', (req, res) => {
  try {
    const { service, type } = req.body;
    const runId = startTestRun(service, type);
    res.json({ runId });
  } catch (e: any) {
    res.status(400).json({ message: e.message });
  }
});

// Stream test output via SSE
router.get('/stream/:runId', (req, res) => {
  const { runId } = req.params;
  const run = getTestRun(runId);
  if (!run) {
    res.status(404).json({ message: 'Test run not found' });
    return;
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  let lastLength = 0;

  const interval = setInterval(() => {
    const state = getTestRun(runId);
    if (!state) {
      clearInterval(interval);
      res.end();
      return;
    }

    // Send new output
    if (state.output.length > lastLength) {
      const newOutput = state.output.slice(lastLength);
      lastLength = state.output.length;
      res.write(`data: ${JSON.stringify({ type: 'output', data: newOutput })}\n\n`);
    }

    // Send status update
    if (state.status !== 'running') {
      res.write(
        `data: ${JSON.stringify({
          type: 'complete',
          status: state.status,
          passed: state.passed,
          failed: state.failed,
          errors: state.errors,
        })}\n\n`,
      );
      clearInterval(interval);
      res.end();
    }
  }, 500);

  req.on('close', () => clearInterval(interval));
});

export default router;
