import { Router } from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';
import { config } from '../config.js';
import { writeClient } from '../services/writeClient.js';

const router = Router();

// Import single image via BFF (upload URL → write-service)
router.post('/import', async (req, res) => {
  try {
    const { url, merchant_id, category_l1, product_id, tags } = req.body;
    const apiKey = req.headers['x-api-key'];
    const resp = await writeClient.post(
      '/api/v1/image/update',
      { uri: url, merchant_id, category_l1, product_id, tags },
      { headers: apiKey ? { 'X-API-Key': apiKey as string } : {} },
    );
    res.json(resp.data);
  } catch (e: any) {
    const status = e.response?.status || 500;
    res.status(status).json(e.response?.data || { message: e.message });
  }
});

export default router;
