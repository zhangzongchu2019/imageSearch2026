#!/usr/bin/env python3
import sys, json, re
import httpx, psycopg2
from pymilvus import connections, Collection

b64 = sys.stdin.read().strip()
r = httpx.post("http://172.21.0.8:8090/api/v1/extract", json={"image_b64": b64}, timeout=30)
d = r.json()
vec = d.get("vector") or d.get("global_vec")
if not vec: print(json.dumps({"error":"no vector"})); sys.exit(0)
vec = vec[:256]

L1 = {0:"未分类",1001:"服装",1002:"箱包",1003:"鞋帽",1004:"珠宝首饰",1005:"房地产",
      1006:"五金建材",1007:"家具",1008:"化妆品",1009:"小家电",1010:"手机",1011:"电脑",
      1012:"食品",1013:"玩具",1014:"运动户外",1015:"汽车配件",1016:"办公用品",1017:"钟表",1018:"眼镜"}

connections.connect("default", host="localhost", port=19530, timeout=10)
coll = Collection("img_202603_svip")
# coll.load()  # pre-loaded
res = coll.search([vec], "global_vec", param={"metric_type":"COSINE","params":{"ef":64}},
                   limit=20, output_fields=["image_pk","category_l1","category_l2","is_evergreen"])

results = []
pks = []
for h in res[0]:
    pk = h.id
    pks.append(pk)
    results.append({
        "pk": pk,
        "score": round(h.score, 4),
        "category": L1.get(h.entity.get("category_l1") or 0, "未分类"),
        "evergreen": bool(h.entity.get("is_evergreen")),
        "url": ""
    })
# coll.release()  # keep loaded

# PG 查图片 URL
conn = psycopg2.connect("postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search", connect_timeout=3)
cur = conn.cursor()
ph = ",".join(["%s"] * len(pks))
cur.execute(f"SELECT image_pk, uri FROM uri_dedup WHERE image_pk IN ({ph})", pks)
uri_map = dict(cur.fetchall())

# 追溯衍生 → 原始
need_resolve = {}
for pk in pks:
    uri = uri_map.get(pk, "")
    if uri.startswith("http"):
        for r in results:
            if r["pk"] == pk: r["url"] = uri; break
    elif uri.startswith("synthetic://"):
        m = re.match(r"synthetic://([a-f0-9]+)/", uri)
        if m: need_resolve[pk] = m.group(1)

if need_resolve:
    orig_list = list(set(need_resolve.values()))
    ph2 = ",".join(["%s"] * len(orig_list))
    cur.execute(f"SELECT image_pk, uri FROM uri_dedup WHERE image_pk IN ({ph2}) AND uri LIKE 'http%%'", orig_list)
    orig_urls = dict(cur.fetchall())
    for pk, orig_pk in need_resolve.items():
        url = orig_urls.get(orig_pk, "")
        if url:
            for r in results:
                if r["pk"] == pk: r["url"] = url; break

cur.close(); conn.close()
print(json.dumps({"results": results}, ensure_ascii=False))
