import os, glob, numpy as np, torch
from PIL import Image

BRANDS_DIR = "/data/imgsrch/brand_products"
MEAN = np.array([0.485,0.456,0.406], dtype=np.float32)
STD = np.array([0.229,0.224,0.225], dtype=np.float32)

def pp(img):
    if img.size != (224,224): img = img.resize((224,224), Image.BICUBIC)
    return ((np.array(img, dtype=np.float32)/255.0 - MEAN) / STD).transpose(2,0,1)

sscd = torch.jit.load("/data/imgsrch/models/sscd_resnet50.pt").cuda().eval()
from transformers import AutoModel
dinov2 = AutoModel.from_pretrained("/data/imgsrch/models/dinov2-large", dtype=torch.float16).cuda().eval()
print("Models loaded")

# Collect products
products = {}
for bd in sorted(glob.glob(os.path.join(BRANDS_DIR, "*"))):
    if not os.path.isdir(bd): continue
    for f in sorted(os.listdir(bd)):
        if not f.endswith(".jpg"): continue
        pid = f.rsplit("_v",1)[0]
        key = f"{os.path.basename(bd)}/{pid}"
        if key not in products: products[key] = []
        products[key].append(os.path.join(bd, f))
multi = {k:v for k,v in products.items() if len(v)>=2}
print(f"Products: {len(multi)}, images: {sum(len(v) for v in multi.values())}")

# Extract vectors
all_paths = []
all_keys = []
for key, paths in multi.items():
    for i, p in enumerate(paths):
        all_paths.append(p)
        all_keys.append((key, i))

BS = 64
sv_list, dv_list = [], []
for s in range(0, len(all_paths), BS):
    batch = all_paths[s:s+BS]
    imgs = []
    for p in batch:
        try: imgs.append(Image.open(p).convert("RGB"))
        except: imgs.append(Image.new("RGB",(224,224)))
    arrs = np.stack([pp(img) for img in imgs])
    t = torch.from_numpy(arrs).cuda()
    with torch.no_grad():
        sf = sscd(t); sf = sf/sf.norm(dim=-1,keepdim=True)
    sv_list.append(sf.cpu().numpy())
    t2 = torch.from_numpy(arrs).cuda().to(torch.float16)
    with torch.no_grad():
        o = dinov2(pixel_values=t2)
        df = o.last_hidden_state[:,0,:]; df = df/df.norm(dim=-1,keepdim=True)
    dv_list.append(df.cpu().float().numpy())
    if (s//BS) % 10 == 0: print(f"  {s}/{len(all_paths)}")

sv = np.concatenate(sv_list)
dv = np.concatenate(dv_list)
print(f"Vectors: {sv.shape}")

# Evaluate
pidx = {}
for i,(k,v) in enumerate(all_keys):
    if k not in pidx: pidx[k]=[]
    pidx[k].append(i)

for name, vecs in [("SSCD",sv),("DINOv2",dv)]:
    rat = {1:0,3:0,5:0,10:0,15:0}
    n = 0
    for key, indices in pidx.items():
        if len(indices)<2: continue
        qi = indices[0]
        targets = set(indices[1:])
        sims = vecs @ vecs[qi]
        sims[qi] = -1
        ranked = np.argsort(-sims)
        for k in rat:
            if set(ranked[:k].tolist()) & targets: rat[k]+=1
        n += 1
    print(f"\n[{name}] {n} products:")
    for k in sorted(rat): print(f"  R@{k}: {rat[k]/n*100:.1f}%")
