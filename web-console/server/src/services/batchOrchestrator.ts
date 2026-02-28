import type { Response } from 'express';
import { writeClient } from './writeClient.js';
import { config } from '../config.js';

interface BatchItem {
  url: string;
  merchant_id: string;
  category_l1: string;
  product_id: string;
  tags?: string[];
}

interface BatchProgress {
  completed: number;
  total: number;
  succeeded: number;
  failed: number;
  results: Array<{
    index: number;
    success: boolean;
    image_id?: string;
    is_new?: boolean;
    error?: string;
  }>;
}

export async function orchestrateBatchImport(
  items: BatchItem[],
  apiKey: string | undefined,
  res: Response,
) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const total = Math.min(items.length, config.batchMaxSize);
  const progress: BatchProgress = { completed: 0, total, succeeded: 0, failed: 0, results: [] };

  const send = () => {
    res.write(`data: ${JSON.stringify(progress)}\n\n`);
  };

  send();

  // Process in batches of batchConcurrency
  for (let i = 0; i < total; i += config.batchConcurrency) {
    const batch = items.slice(i, i + config.batchConcurrency);
    const promises = batch.map(async (item, j) => {
      const index = i + j;
      try {
        const resp = await writeClient.post(
          '/api/v1/image/update',
          { uri: item.url, merchant_id: item.merchant_id, category_l1: item.category_l1, product_id: item.product_id, tags: item.tags },
          { headers: apiKey ? { 'X-API-Key': apiKey } : {} },
        );
        progress.succeeded++;
        progress.results.push({ index, success: true, image_id: resp.data.image_id, is_new: resp.data.is_new });
      } catch (e: any) {
        progress.failed++;
        progress.results.push({ index, success: false, error: e.response?.data?.error?.message || e.message });
      }
      progress.completed++;
      send();
    });
    await Promise.allSettled(promises);
  }

  res.write('event: done\ndata: {}\n\n');
  res.end();
}
