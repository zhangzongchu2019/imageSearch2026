import { Router } from 'express';
import axios from 'axios';
import { config } from '../config.js';

const router = Router();
const cronClient = axios.create({ baseURL: config.cronServiceUrl, timeout: 10000 });

router.get('/jobs', async (_req, res) => {
  try {
    const { data } = await cronClient.get('/api/v1/jobs');
    res.json(data);
  } catch (e: any) {
    res.status(502).json({ message: 'cron-scheduler unavailable: ' + e.message });
  }
});

router.post('/jobs/:name/trigger', async (req, res) => {
  try {
    const { data } = await cronClient.post(`/api/v1/jobs/${req.params.name}/trigger`);
    res.json(data);
  } catch (e: any) {
    res.status(502).json({ message: e.message });
  }
});

router.get('/jobs/:name/history', async (req, res) => {
  try {
    const { data } = await cronClient.get(`/api/v1/jobs/${req.params.name}/history`);
    res.json(data);
  } catch (e: any) {
    res.status(502).json({ message: e.message });
  }
});

export default router;
