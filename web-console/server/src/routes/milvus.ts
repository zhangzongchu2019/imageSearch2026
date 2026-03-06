import { Router } from 'express';
import { MilvusClient } from '@zilliz/milvus2-sdk-node';

const router = Router();

const MILVUS_ADDRESS = `${process.env.MILVUS_HOST || 'localhost'}:${process.env.MILVUS_PORT || '19530'}`;
const COLLECTION = 'global_images_hot';

async function withMilvus<T>(fn: (client: MilvusClient) => Promise<T>): Promise<T> {
  const client = new MilvusClient({ address: MILVUS_ADDRESS });
  try {
    await client.connectPromise;
    return await fn(client);
  } finally {
    await client.closeConnection().catch(() => {});
  }
}

// GET /partitions — list partitions with counts
router.get('/partitions', async (_req, res) => {
  try {
    const result = await withMilvus(async (client) => {
      await client.flushSync({ collection_names: [COLLECTION] });
      const showRes = await client.showPartitions({ collection_name: COLLECTION });
      const partitions = showRes.data || [];

      // Get row count for each partition
      const items = await Promise.all(
        partitions.map(async (p: any) => {
          let count = 0;
          try {
            const stats = await client.getPartitionStatistics({
              collection_name: COLLECTION,
              partition_name: p.name,
            });
            const rowCountStat = (stats.stats || []).find((s: any) => s.key === 'row_count');
            count = rowCountStat ? Number(rowCountStat.value) : 0;
          } catch {
            // ignore stats error
          }
          return { name: p.name, count };
        })
      );
      return items;
    });
    res.json(result);
  } catch (e: any) {
    console.error('[milvus] /partitions error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /data?partition=xxx&offset=0&limit=200
router.get('/data', async (req, res) => {
  const partition = req.query.partition as string;
  const offset = parseInt(req.query.offset as string) || 0;
  const limit = Math.min(parseInt(req.query.limit as string) || 200, 200);

  if (!partition) {
    res.status(400).json({ error: 'partition is required' });
    return;
  }

  if (!/^[a-zA-Z0-9_]+$/.test(partition)) {
    res.status(400).json({ error: 'invalid partition name' });
    return;
  }

  try {
    const result = await withMilvus(async (client) => {
      const fields = ['image_pk', 'product_id', 'is_evergreen', 'category_l1', 'category_l2', 'tags', 'ts_month'];

      const queryRes = await client.query({
        collection_name: COLLECTION,
        partition_names: [partition],
        expr: "image_pk != ''",
        output_fields: fields,
        limit,
        offset,
      });

      // Get partition count
      let total = 0;
      try {
        const stats = await client.getPartitionStatistics({
          collection_name: COLLECTION,
          partition_name: partition,
        });
        const rowCountStat = (stats.stats || []).find((s: any) => s.key === 'row_count');
        total = rowCountStat ? Number(rowCountStat.value) : 0;
      } catch {
        // ignore
      }

      // Get vector dimension from schema
      const desc = await client.describeCollection({ collection_name: COLLECTION });
      let vecDim: number | null = null;
      for (const f of desc.schema?.fields || []) {
        if (f.name === 'global_vec') {
          const dimParam = (f.type_params || []).find((tp: any) => tp.key === 'dim');
          vecDim = dimParam ? Number(dimParam.value) : null;
          break;
        }
      }

      const records = (queryRes.data || []).map((r: any) => ({
        ...r,
        vec_dim: vecDim,
      }));

      return { records, total };
    });
    res.json(result);
  } catch (e: any) {
    console.error('[milvus] /data error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

export default router;
