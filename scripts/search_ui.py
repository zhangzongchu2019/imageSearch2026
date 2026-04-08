#!/usr/bin/env python3
"""以图搜商品 v3 — COS上传 + 本地上传 + URL搜索 + 品类加权"""
import time, base64, re, json, logging, threading, uuid
from io import BytesIO
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn, httpx, psycopg2
from PIL import Image

app = FastAPI()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("search")

# === 配置 ===
INFERENCE_URL = "http://172.21.0.8:8090"
MILVUS_HOST, MILVUS_PORT = "localhost", 19530
PG_DSN = "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search"
COLL_NAME = "img_202603_svip"
IMG_SIZE = 224

# COS 配置
COS_SECRET_ID = os.environ.get("COS_SECRET_ID", "")
COS_SECRET_KEY = os.environ.get("COS_SECRET_KEY", "")
COS_BUCKET = "test-1251632793"
COS_REGION = "ap-guangzhou"

L1 = {0:"未分类",1001:"服装",1002:"箱包",1003:"鞋帽",1004:"珠宝首饰",1005:"房地产",
      1006:"五金建材",1007:"家具",1008:"化妆品",1009:"小家电",1010:"手机",1011:"电脑",
      1012:"食品",1013:"玩具",1014:"运动户外",1015:"汽车配件",1016:"办公用品",1017:"钟表",1018:"眼镜"}
L2 = {100101:"上装",100102:"下装",100103:"连衣裙",100104:"外套",100105:"内衣",100106:"运动服",100107:"套装",100201:"手提包",100202:"双肩包",100203:"单肩包",100204:"钱包",100205:"旅行箱",100206:"腰包",100301:"运动鞋",100302:"皮鞋",100303:"高跟鞋",100304:"靴子",100305:"凉鞋拖鞋",100306:"帽子",100401:"戒指",100402:"项链",100403:"手镯手链",100404:"耳环耳饰",100405:"翡翠玉器",100406:"黄金饰品",100407:"钻石宝石",100408:"胸针别针",100501:"住宅",100502:"商铺",100503:"室内装修",100504:"户型图",100601:"水暖管件",100602:"电气配件",100603:"工具",100604:"装饰建材",100605:"门窗五金",100701:"沙发",100702:"桌子",100703:"椅子",100704:"柜子",100705:"床",100706:"灯具",100801:"彩妆",100802:"护肤",100803:"香水",100804:"美容工具",100805:"身体护理",100901:"厨房小家电",100902:"个人护理",100903:"清洁电器",100904:"生活电器",101001:"智能手机",101002:"手机配件",101003:"平板电脑",101101:"笔记本电脑",101102:"台式电脑",101103:"电脑配件",101104:"存储设备",101201:"零食",101202:"饮品",101203:"保健品",101204:"调味品",101301:"积木拼图",101302:"玩偶公仔",101303:"遥控玩具",101304:"益智玩具",101401:"健身器材",101402:"户外装备",101403:"球类运动",101404:"骑行装备",101501:"车内饰品",101502:"车外配件",101503:"汽车电子",101504:"汽车养护",101601:"文具",101602:"打印耗材",101603:"办公设备",101701:"机械表",101702:"石英表",101703:"智能手表",101704:"座钟挂钟",101801:"太阳镜",101802:"近视眼镜",101803:"镜框镜片"}

from pymilvus import connections, Collection
connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
_coll = Collection(COLL_NAME); _coll.load()
log.info(f"{COLL_NAME} pre-loaded")
_lock = threading.Lock()

def resize_image(img_bytes, size=IMG_SIZE):
    try:
        from pillow_heif import register_heif_opener
        register_heif_opener()
    except: pass
    img = Image.open(BytesIO(img_bytes)).convert("RGB")
    img = img.resize((size, size), Image.LANCZOS)
    buf = BytesIO(); img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()

def _do_search(image_b64, use_category_boost=True):
    global _coll
    t_inf = time.time()
    r = httpx.post(f"{INFERENCE_URL}/api/v1/extract", json={"image_b64": image_b64}, timeout=15)
    if r.status_code != 200: return None, f"推理失败 {r.status_code}"
    d = r.json()
    vec = (d.get("global_vec") or d.get("vector"))[:256]
    query_l1 = d.get("category_l1_pred", 0) or 0
    query_l2 = d.get("category_l2_pred", 0) or 0
    t_inf_done = time.time()

    t_search = time.time()
    for attempt in range(2):
        try:
            with _lock:
                res1 = _coll.search([vec], "global_vec", param={"metric_type":"COSINE","params":{"ef":64}},
                                    limit=20, output_fields=["image_pk","category_l1","category_l2","is_evergreen"])
                res2 = None
                if use_category_boost and query_l1 > 0:
                    try:
                        res2 = _coll.search([vec], "global_vec", param={"metric_type":"COSINE","params":{"ef":64}},
                                            limit=20, expr=f"category_l1 == {query_l1}",
                                            output_fields=["image_pk","category_l1","category_l2","is_evergreen"])
                    except: pass
            break
        except:
            if attempt == 0:
                try: connections.disconnect("default")
                except: pass
                connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=10)
                _coll = Collection(COLL_NAME); _coll.load(); time.sleep(2)
            else: return None, "搜索失败"
    t_search_done = time.time()

    seen = set(); results_all = []
    for h in res1[0]:
        pk = h.id
        if pk in seen: continue
        seen.add(pk)
        l1c = h.entity.get("category_l1") or 0; l2c = h.entity.get("category_l2") or 0
        boost = (0.05 if l1c == query_l1 and query_l1 > 0 else 0) + (0.03 if l2c == query_l2 and query_l2 > 0 else 0)
        results_all.append({"pk":pk,"score":round(h.score+boost,4),"raw_score":round(h.score,4),
                            "category":L1.get(l1c,f"#{l1c}"),"category_l2":L2.get(l2c,""),"l1":l1c,"l2":l2c,
                            "evergreen":bool(h.entity.get("is_evergreen")),"url":"","boosted":boost>0})
    if res2:
        for h in res2[0]:
            pk = h.id
            if pk in seen: continue
            seen.add(pk)
            l1c = h.entity.get("category_l1") or 0; l2c = h.entity.get("category_l2") or 0
            results_all.append({"pk":pk,"score":round(h.score+0.05,4),"raw_score":round(h.score,4),
                                "category":L1.get(l1c,f"#{l1c}"),"category_l2":L2.get(l2c,""),"l1":l1c,"l2":l2c,
                                "evergreen":bool(h.entity.get("is_evergreen")),"url":"","boosted":True})
    results_all.sort(key=lambda x: x["score"], reverse=True)
    results_all = [r for r in results_all if r['raw_score'] >= 0.80][:20]

    t_pg = time.time()
    pks = [r["pk"] for r in results_all]
    try:
        conn = psycopg2.connect(PG_DSN, connect_timeout=2); cur = conn.cursor()
        ph = ",".join(["%s"]*len(pks))
        cur.execute(f"SELECT image_pk, uri FROM uri_dedup WHERE image_pk IN ({ph})", pks)
        uri_map = dict(cur.fetchall())
        need = {}
        for pk in pks:
            uri = uri_map.get(pk,"")
            if uri.startswith("http"):
                for r in results_all:
                    if r["pk"]==pk: r["url"]=uri; break
            elif uri.startswith("synthetic://"):
                m = re.match(r"synthetic://([a-f0-9]+)/", uri)
                if m: need[pk] = m.group(1)
        if need:
            ol = list(set(need.values()))
            cur.execute(f"SELECT image_pk, uri FROM uri_dedup WHERE image_pk IN ({','.join(['%s']*len(ol))}) AND uri LIKE 'http%%'", ol)
            ou = dict(cur.fetchall())
            for pk, op in need.items():
                if op in ou:
                    for r in results_all:
                        if r["pk"]==pk: r["url"]=ou[op]; break
        cur.close(); conn.close()
    except: pass
    t_pg_done = time.time()

    timing = {"inference":round((t_inf_done-t_inf)*1000),"search":round((t_search_done-t_search)*1000),"pg":round((t_pg_done-t_pg)*1000)}
    return {"results":results_all,"query_category":d.get("category_l1",""),"query_l1":query_l1,"query_l2":query_l2,"timing":timing}, None


# === COS STS 临时密钥 ===
@app.get("/api/cos/sts")
async def cos_sts():
    from sts.sts import Sts
    config = {
        'secret_id': COS_SECRET_ID, 'secret_key': COS_SECRET_KEY,
        'duration_seconds': 1800, 'bucket': COS_BUCKET, 'region': COS_REGION,
        'allow_prefix': ['imgsrch/*'],
        'allow_actions': ['name/cos:PutObject','name/cos:PostObject','name/cos:InitiateMultipartUpload',
                          'name/cos:ListMultipartUploads','name/cos:ListParts','name/cos:UploadPart',
                          'name/cos:CompleteMultipartUpload'],
    }
    resp = Sts(config).get_credential()
    return {"credentials": resp["credentials"], "startTime": resp["startTime"],
            "expiredTime": resp["expiredTime"], "bucket": COS_BUCKET, "region": COS_REGION}


# === 搜索 API ===
@app.post("/api/search/upload")
async def api_search_upload(file: UploadFile = File(...)):
    import asyncio
    t0 = time.time()
    content = await file.read()
    if len(content) > 10*1024*1024: return JSONResponse({"error":"图片过大"}, status_code=400)
    try: resized = resize_image(content)
    except Exception as e:
        log.warning(f"resize failed: {e}, trying raw image")
        # resize 失败时直接用原图 (可能是 HEIC/WebP 等格式)
        try:
            resized = content  # 让 inference 的 preprocess 处理
        except:
            return JSONResponse({"error":f"图片处理失败: {e}"}, status_code=400)
    b64 = base64.b64encode(resized).decode()
    try:
        data, err = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, _do_search, b64), timeout=15)
    except: return JSONResponse({"error":"超时"}, status_code=503)
    if err: return JSONResponse({"error":err}, status_code=500)
    data["latency_ms"] = round((time.time()-t0)*1000); data["count"] = len(data["results"]); data["method"] = "upload"
    return data

@app.post("/api/search")
async def api_search_post(request: Request):
    import asyncio
    t0 = time.time()
    body = await request.json(); b64 = body.get("image_b64","")
    if not b64: return JSONResponse({"error":"缺少 image_b64"}, status_code=400)
    try:
        raw = base64.b64decode(b64); resized = resize_image(raw); b64 = base64.b64encode(resized).decode()
    except Exception as e:
        log.warning(f"resize b64 failed: {e}, using original")
    try:
        data, err = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, _do_search, b64), timeout=15)
    except: return JSONResponse({"error":"超时"}, status_code=503)
    if err: return JSONResponse({"error":err}, status_code=500)
    data["latency_ms"] = round((time.time()-t0)*1000); data["count"] = len(data["results"])
    return data

@app.get("/api/search")
async def api_search_get(url: str):
    import asyncio
    t0 = time.time()
    try:
        # COS 缩图: ?imageMogr2/thumbnail/224x224
        dl_url = url
        if "myqcloud.com" in url and "imageMogr2" not in url:
            dl_url += ("&" if "?" in url else "?") + "imageMogr2/thumbnail/224x224"
        r = httpx.get(dl_url, timeout=10, follow_redirects=True)
        if r.status_code != 200: return JSONResponse({"error":f"HTTP {r.status_code}"}, status_code=400)
        b64 = base64.b64encode(r.content).decode()
    except Exception as e:
        return JSONResponse({"error":str(e)[:80]}, status_code=400)
    try:
        data, err = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, _do_search, b64), timeout=15)
    except: return JSONResponse({"error":"超时"}, status_code=503)
    if err: return JSONResponse({"error":err}, status_code=500)
    data["url"] = url; data["latency_ms"] = round((time.time()-t0)*1000); data["count"] = len(data["results"])
    return data


# === 前端页面 ===
@app.get("/", response_class=HTMLResponse)
async def index():
    return """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>以图搜商品</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,sans-serif;background:#0d1117;color:#e6edf3;padding:12px;max-width:900px;margin:0 auto}
h1{font-size:20px;color:#58a6ff;margin-bottom:12px;text-align:center}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;margin-bottom:10px}
.card h2{font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px}
.btn{background:#1f6feb;color:#fff;border:none;padding:12px;border-radius:6px;font-size:15px;cursor:pointer;width:100%;margin:6px 0}
.btn:hover{background:#388bfd}.btn:disabled{opacity:.5;cursor:wait}
.btn-green{background:#238636}.btn-green:hover{background:#2ea043}
.btn-orange{background:#d29922}.btn-orange:hover{background:#e3b341}
input[type=text]{width:100%;padding:10px;background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#e6edf3;font-size:14px;margin:4px 0}
.upload-area{border:2px dashed #30363d;border-radius:8px;padding:24px;text-align:center;cursor:pointer;transition:.3s}
.upload-area:hover{border-color:#58a6ff}
.upload-area.has-file{border-color:#3fb950;background:rgba(63,185,80,.05)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}
.item{background:#0d1117;border:1px solid #21262d;border-radius:8px;overflow:hidden}
.item.boosted{border-color:#d29922}
.item img{width:100%;height:150px;object-fit:cover;display:block}
.item .m{padding:6px;font-size:11px;text-align:center}
.item .sc{color:#3fb950;font-weight:700;font-size:15px}
.item .cat{color:#d29922}.item .rk{color:#58a6ff;font-size:10px}
.noimg{width:100%;height:150px;background:#21262d;display:flex;align-items:center;justify-content:center;color:#484f58;font-size:11px}
.foot{color:#484f58;font-size:11px;text-align:center;margin-top:6px}
.timing{font-size:10px;color:#484f58;text-align:center;margin-top:4px}
#ld{display:none;text-align:center;padding:20px;color:#58a6ff}
.tabs{display:flex;gap:4px;margin-bottom:10px}
.tab{flex:1;padding:8px;text-align:center;background:#21262d;border-radius:6px;cursor:pointer;font-size:13px;color:#8b949e}
.tab.active{background:#1f6feb;color:#fff}
/* 多图缩略图栏 */
.thumb-bar{display:flex;gap:6px;overflow-x:auto;padding:8px 0;margin-bottom:8px}
.thumb{width:60px;height:60px;border-radius:6px;object-fit:cover;cursor:pointer;border:2px solid #30363d;flex-shrink:0;transition:.2s}
.thumb.active{border-color:#58a6ff;box-shadow:0 0 8px rgba(88,166,255,.4)}
.thumb.loading{opacity:.5;animation:pulse 1s infinite}
.thumb.done{border-color:#3fb950}
.thumb.error{border-color:#f85149}
@keyframes pulse{0%,100%{opacity:.5}50%{opacity:1}}
.badge{position:absolute;top:2px;right:2px;font-size:9px;background:#238636;color:#fff;padding:1px 4px;border-radius:4px}
.thumb-wrap{position:relative;flex-shrink:0}
</style>
<script src="https://cdn.jsdelivr.net/npm/cos-js-sdk-v5/dist/cos-js-sdk-v5.min.js"></script>
</head><body>
<h1>&#128269; 以图搜商品 v3</h1>
<div class="card">
<div class="tabs">
<div class="tab active" onclick="switchTab(0)">&#128247; 本地上传</div>
<div class="tab" onclick="switchTab(1)">&#9729; COS上传</div>
<div class="tab" onclick="switchTab(2)">&#128279; 图片URL</div>
</div>

<div id="tab0">
<div class="upload-area" id="dz0" onclick="document.getElementById('fi0').click()">
<div style="font-size:36px;margin-bottom:6px">&#128247;</div>
<div>选择/拍照/拖拽 (支持多图)</div>
<div style="font-size:11px;color:#484f58">自动 resize 224x224</div>
</div>
<input type="file" id="fi0" accept="image/*" multiple style="display:none" onchange="addFiles(this.files,0)">
<button class="btn btn-green" id="sb0" style="display:none" onclick="searchBatch(0)">&#128269; 搜索全部</button>
</div>

<div id="tab1" style="display:none">
<div class="upload-area" id="dz1" onclick="document.getElementById('fi1').click()">
<div style="font-size:36px;margin-bottom:6px">&#9729;</div>
<div>选择/拍照 (支持多图) → COS</div>
</div>
<input type="file" id="fi1" accept="image/*" multiple style="display:none" onchange="addFiles(this.files,1)">
<button class="btn btn-orange" id="sb1" style="display:none" onclick="searchBatch(1)">&#9729; 上传COS并搜索</button>
</div>

<div id="tab2" style="display:none">
<textarea id="urls" rows="3" placeholder="每行一个图片URL..." style="width:100%;padding:10px;background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#e6edf3;font-size:13px;resize:vertical"></textarea>
<button class="btn" onclick="searchUrls()">&#128269; 搜索全部URL</button>
</div>
</div>

<!-- 多图缩略图栏 -->
<div class="thumb-bar" id="thumbs" style="display:none"></div>

<div id="ld">&#9203; 搜索中... <span id="ldprog"></span></div>
<div class="card" id="rc" style="display:none">
<h2>相似商品 <span id="rm" style="float:right;font-weight:400;font-size:11px"></span></h2>
<div class="grid" id="rs"></div>
<div class="timing" id="tm"></div>
</div>
<div class="foot">ViT-L-14 + FashionSigLIP &#xB7; Milvus分布式 &#xB7; 品类加权 &#xB7; 多图批量</div>

<script>
const B=window.location.pathname.replace(/\\/$/,'');
let allJobs=[];  // [{file, preview, status:'pending'|'loading'|'done'|'error', result:null}]
let activeIdx=0;

function switchTab(n){document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',i===n));
[0,1,2].forEach(i=>document.getElementById('tab'+i).style.display=i===n?'block':'none')}

// 拖拽
['dz0','dz1'].forEach(id=>{const el=document.getElementById(id);if(!el)return;
el.addEventListener('dragover',e=>{e.preventDefault();el.style.borderColor='#3fb950'});
el.addEventListener('dragleave',()=>el.style.borderColor='#30363d');
el.addEventListener('drop',e=>{e.preventDefault();el.style.borderColor='#30363d';addFiles(e.dataTransfer.files,id==='dz0'?0:1)})});

function addFiles(fileList, tabIdx){
const newFiles=Array.from(fileList).filter(f=>f.type.startsWith('image/'));
if(!newFiles.length)return;
newFiles.forEach(f=>{
const reader=new FileReader();
reader.onload=e=>{
allJobs.push({file:f, preview:e.target.result, status:'pending', result:null, method:tabIdx===0?'local':'cos'});
renderThumbs();
document.getElementById('sb'+tabIdx).style.display='block'};
reader.readAsDataURL(f)});
const dz=document.getElementById('dz'+tabIdx);
dz.classList.add('has-file');dz.innerHTML='<div style="color:#3fb950">&#x2705; '+newFiles.length+' 张图片</div>'}

function renderThumbs(){
const bar=document.getElementById('thumbs');
if(!allJobs.length){bar.style.display='none';return}
bar.style.display='flex';
bar.innerHTML=allJobs.map((j,i)=>{
const cls='thumb'+(i===activeIdx?' active':'')+(j.status==='loading'?' loading':'')+(j.status==='done'?' done':'')+(j.status==='error'?' error':'');
return '<div class="thumb-wrap"><img class="'+cls+'" src="'+j.preview+'" onclick="selectThumb('+i+')"></div>'}).join('')}

function selectThumb(idx){
activeIdx=idx;renderThumbs();
const j=allJobs[idx];
if(j.result){showResult(j.result,0,j.method)}
else if(j.status==='pending'){document.getElementById('rc').style.display='none'}
else if(j.status==='loading'){document.getElementById('rc').style.display='none';document.getElementById('ld').style.display='block'}}

// 批量搜索
async function searchBatch(tabIdx){
const method=tabIdx===0?'local':'cos';
const pending=allJobs.filter(j=>j.status==='pending'&&j.method===method);
if(!pending.length){alert('没有待搜索的图片');return}
document.getElementById('sb'+tabIdx).disabled=true;

let stsData=null;
if(method==='cos'){
try{stsData=await(await fetch(B+'/api/cos/sts')).json()}catch(e){alert('STS失败');return}}

let done=0;
for(const job of pending){
job.status='loading';renderThumbs();
document.getElementById('ld').style.display='block';
document.getElementById('ldprog').textContent='('+(++done)+'/'+pending.length+')';
const t0=Date.now();
try{
let d;
if(method==='local'){
const fd=new FormData();fd.append('file',job.file);
d=await(await fetch(B+'/api/search/upload',{method:'POST',body:fd})).json()}
else{
const cos=new COS({getAuthorization:(o,cb)=>cb({TmpSecretId:stsData.credentials.tmpSecretId,
TmpSecretKey:stsData.credentials.tmpSecretKey,SecurityToken:stsData.credentials.sessionToken,
StartTime:stsData.startTime,ExpiredTime:stsData.expiredTime})});
const key='imgsrch/'+Date.now()+'_'+Math.random().toString(36).slice(2,8)+'.jpg';
await new Promise((res,rej)=>cos.uploadFile({Bucket:stsData.bucket,Region:stsData.region,Key:key,Body:job.file,SliceSize:1024*1024*5},(e,d)=>e?rej(e):res(d)));
const cosUrl='https://'+stsData.bucket+'.cos.'+stsData.region+'.myqcloud.com/'+key;
d=await(await fetch(B+'/api/search?url='+encodeURIComponent(cosUrl))).json()}
if(d.error){job.status='error';job.result={error:d.error}}
else{job.status='done';d.latency_ms=Date.now()-t0;d.count=(d.results||[]).length;job.result=d}
}catch(e){job.status='error';job.result={error:String(e)}}
renderThumbs();
// 第一个完成时自动显示
if(done===1||activeIdx===allJobs.indexOf(job)){activeIdx=allJobs.indexOf(job);if(job.result&&!job.result.error)showResult(job.result,0,method)}}
document.getElementById('ld').style.display='none';
document.getElementById('sb'+tabIdx).disabled=false}

// URL 批量
async function searchUrls(){
const text=document.getElementById('urls').value.trim();
if(!text){alert('请输入URL');return}
const urls=text.split('\\n').map(u=>u.trim()).filter(u=>u.match(/^https?:\\/\\//));
if(!urls.length){alert('没有有效URL');return}
// 转为 jobs
urls.forEach(url=>allJobs.push({file:null,preview:url,status:'pending',result:null,method:'url',url:url}));
renderThumbs();
let done=0;
for(const job of allJobs.filter(j=>j.status==='pending'&&j.method==='url')){
job.status='loading';renderThumbs();
document.getElementById('ld').style.display='block';
document.getElementById('ldprog').textContent='('+(++done)+'/'+urls.length+')';
const t0=Date.now();
try{
const img=await fetch(job.url).then(r=>r.ok?r.blob():null).catch(()=>null);
let d;
if(img&&img.size>0){
const b64=await new Promise(res=>{const r=new FileReader();r.onload=()=>res(r.result.split(',')[1]);r.readAsDataURL(img)});
d=await(await fetch(B+'/api/search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({image_b64:b64})})).json()}
else{d=await(await fetch(B+'/api/search?url='+encodeURIComponent(job.url))).json()}
if(d.error){job.status='error'}else{job.status='done';d.latency_ms=Date.now()-t0;d.count=(d.results||[]).length;job.result=d}
}catch(e){job.status='error';job.result={error:String(e)}}
renderThumbs();
if(done===1||activeIdx===allJobs.indexOf(job)){activeIdx=allJobs.indexOf(job);if(job.result&&!job.result.error)showResult(job.result,0,'url')}}
document.getElementById('ld').style.display='none'}

function showResult(d,t0_unused,method){
if(d.error){alert(d.error);return}
document.getElementById('rc').style.display='block';
document.getElementById('rm').innerHTML=(d.latency_ms||'?')+'ms | '+(d.count||0)+'条'+(d.query_category?' | '+d.query_category:'');
document.getElementById('tm').textContent=d.timing?'推理'+d.timing.inference+'ms + 搜索'+d.timing.search+'ms + 查图'+d.timing.pg+'ms':'';
document.getElementById('rs').innerHTML=(d.results||[]).map((r,i)=>{
const img=r.url?'<img src="'+r.url+'" onerror="this.outerHTML=\\'<div class=noimg>加载失败</div>\\'">':'<div class="noimg">无图</div>';
return '<div class="item'+(r.boosted?' boosted':'')+'">'+img+'<div class="m"><span class="rk">#'+(i+1)+'</span> <span class="sc">'+r.score+'</span><br><span class="cat">'+r.category+(r.category_l2?' / '+r.category_l2:'')+'</span>'+(r.evergreen?' &#x1F33F;':'')+'</div></div>'}).join('')||'无结果';
document.getElementById('ld').style.display='none'}
</script></body></html>"""

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9810, log_level="info")
