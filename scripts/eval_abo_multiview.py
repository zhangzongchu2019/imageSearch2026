import os, json, numpy as np, torch
from PIL import Image

DIR = "/data/datasets/abo/products"
MEAN = np.array([0.485,0.456,0.406], dtype=np.float32)
STD = np.array([0.229,0.224,0.225], dtype=np.float32)

def pp(img):
    if img.size!=(224,224): img=img.resize((224,224),Image.BICUBIC)
    return ((np.array(img,dtype=np.float32)/255.0-MEAN)/STD).transpose(2,0,1)

sscd = torch.jit.load("/data/imgsrch/models/sscd_resnet50.pt").cuda().eval()
from transformers import AutoModel
dinov2 = AutoModel.from_pretrained("/data/imgsrch/models/dinov2-large",dtype=torch.float16).cuda().eval()

with open(os.path.join(DIR,"manifest.json")) as f:
    manifest = json.load(f)
products = manifest["products"]

all_paths, all_keys = [], []
for p in products:
    for i, fname in enumerate(p["files"]):
        fpath = os.path.join(DIR, fname)
        if os.path.exists(fpath):
            all_paths.append(fpath)
            all_keys.append((p["item_id"], i))

BS = 64
sv_list, dv_list = [], []
for s in range(0, len(all_paths), BS):
    batch = all_paths[s:s+BS]
    imgs = [Image.open(p).convert("RGB") if os.path.exists(p) else Image.new("RGB",(224,224)) for p in batch]
    arrs = np.stack([pp(img) for img in imgs])
    t = torch.from_numpy(arrs).cuda()
    with torch.no_grad():
        sf = sscd(t); sf = sf/sf.norm(dim=-1,keepdim=True)
    sv_list.append(sf.cpu().numpy())
    t2 = torch.from_numpy(arrs).cuda().to(torch.float16)
    with torch.no_grad():
        o = dinov2(pixel_values=t2); df = o.last_hidden_state[:,0,:]; df = df/df.norm(dim=-1,keepdim=True)
    dv_list.append(df.cpu().float().numpy())

sv = np.concatenate(sv_list); dv = np.concatenate(dv_list)
pidx = {}
for i,(k,v) in enumerate(all_keys):
    if k not in pidx: pidx[k]=[]
    pidx[k].append(i)

for name, vecs in [("SSCD",sv),("DINOv2",dv)]:
    rat = {1:0,3:0,5:0,10:0,15:0}
    n = 0
    for key, indices in pidx.items():
        if len(indices)<2: continue
        qi = indices[0]; targets = set(indices[1:])
        sims = vecs@vecs[qi]; sims[qi]=-1; ranked = np.argsort(-sims)
        for k in rat:
            if set(ranked[:k].tolist())&targets: rat[k]+=1
        n += 1
    print(f"[{name}] {n} products: " + " ".join(f"R@{k}={rat[k]/n*100:.1f}%" for k in sorted(rat)))
