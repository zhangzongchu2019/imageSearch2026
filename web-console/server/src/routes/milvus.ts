import { Router } from 'express';
import { execFile } from 'child_process';

const router = Router();

const MILVUS_HOST = process.env.MILVUS_HOST || 'localhost';
const MILVUS_PORT = process.env.MILVUS_PORT || '19530';
const COLLECTION = 'global_images_hot';

function runPython(script: string): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile('python3', ['-c', script], { timeout: 30_000 }, (err, stdout, stderr) => {
      if (err) {
        reject(new Error(stderr || err.message));
      } else {
        resolve(stdout);
      }
    });
  });
}

// GET /partitions — list partitions with counts
router.get('/partitions', async (_req, res) => {
  const script = `
import json
from pymilvus import connections, Collection

connections.connect(host="${MILVUS_HOST}", port=${MILVUS_PORT})
coll = Collection("${COLLECTION}")
coll.flush()

result = []
for p in coll.partitions:
    info = {"name": p.name, "count": p.num_entities}
    # try to get last updated time from PG uri_dedup
    try:
        import subprocess, os as _os
        pg_dsn = _os.environ.get("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
        r = subprocess.run(
            ["psql", "-t", "-A", "-c",
             f"SELECT max(created_at) FROM uri_dedup WHERE ts_month = (SELECT max(ts_month) FROM uri_dedup)",
             pg_dsn],
            capture_output=True, text=True, timeout=5
        )
        val = r.stdout.strip()
        if val and val != "":
            info["lastUpdated"] = val
    except Exception:
        pass
    result.append(info)

connections.disconnect("default")
print(json.dumps(result))
`;
  try {
    const out = await runPython(script);
    res.json(JSON.parse(out));
  } catch (e: any) {
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

  // Sanitize partition name (only allow alphanumeric and underscore)
  if (!/^[a-zA-Z0-9_]+$/.test(partition)) {
    res.status(400).json({ error: 'invalid partition name' });
    return;
  }

  const script = `
import json
from pymilvus import connections, Collection

connections.connect(host="${MILVUS_HOST}", port=${MILVUS_PORT})
coll = Collection("${COLLECTION}")
coll.load(partition_names=["${partition}"])

fields = ["image_pk", "product_id", "is_evergreen", "category_l1", "category_l2", "tags", "ts_month"]
rows = coll.query(
    expr="image_pk != ''",
    partition_names=["${partition}"],
    output_fields=fields,
    limit=${limit},
    offset=${offset},
)

# Get partition count
total = 0
for p in coll.partitions:
    if p.name == "${partition}":
        total = p.num_entities
        break

# Get vector dimension
vec_dim = None
for f in coll.schema.fields:
    if f.name == "global_vec":
        vec_dim = f.params.get("dim", None)
        break

for r in rows:
    r["vec_dim"] = vec_dim

connections.disconnect("default")
print(json.dumps({"records": rows, "total": total}))
`;
  try {
    const out = await runPython(script);
    res.json(JSON.parse(out));
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
