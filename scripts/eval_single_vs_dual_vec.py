import os, json, numpy as np, torch
from PIL import Image
from pymilvus import connections, Collection

EVAL = "/data/imgsrch/eval_10scenes"
MEAN = np.array([0.485,0.456,0.406], dtype=np.float32)
STD = np.array([0.229,0.224,0.225], dtype=np.float32)

def pp(img):
    if img.size!=(224,224): img=img.resize((224,224),Image.BICUBIC)
    return ((np.array(img,dtype=np.float32)/255.0-MEAN)/STD).transpose(2,0,1)

def make_crops(img):
    w,h = img.size
    return [
        img.crop((int(w*0.2),int(h*0.2),int(w*0.8),int(h*0.8))),
        img.crop((int(w*0.3),int(h*0.3),int(w*0.7),int(h*0.7))),
        img.crop((0,0,w,int(h*0.5))),
        img.crop((0,int(h*0.5),w,h)),
        img.crop((int(w*0.1),int(h*0.1),int(w*0.9),int(h*0.5))),
        img.crop((int(w*0.1),int(h*0.5),int(w*0.9),int(h*0.9))),
    ]

connections.connect(host="imgsrch-mvs-proxy", port="19530")
coll = Collection("h4v03_main"); coll.load()
sscd = torch.jit.load("/data/imgsrch/models/sscd_resnet50.pt").cuda().eval()
P = {"metric_type":"COSINE","params":{"ef":200}}

with open(os.path.join(EVAL,"ground_truth.json")) as f:
    scenarios = json.load(f)["scenarios"]

for sc in scenarios:
    d = os.path.join(EVAL, sc)
    files = sorted(os.listdir(d))[:200]
    r1=r2=r3=0
    for f in files:
        try:
            pk=f.replace(".jpg","")
            img=Image.open(os.path.join(d,f)).convert("RGB")
        except: continue
        t=torch.from_numpy(pp(img)[np.newaxis]).cuda()
        with torch.no_grad():
            ft=sscd(t); ft=ft/ft.norm(dim=-1,keepdim=True)
        v=ft[0].cpu().numpy()
        res=coll.search([v.tolist()],"sscd_vec",P,15,output_fields=["image_pk"])
        h1={x.entity.get("image_pk"):float(x.score) for x in res[0]}
        t1=float(res[0][0].score) if res[0] else 0
        if pk in list(h1.keys())[:10]: r1+=1
        if t1<0.8:
            crops=make_crops(img)
            ca=[pp(c) for c in crops]
            ct=torch.from_numpy(np.stack(ca)).cuda()
            with torch.no_grad():
                cf=sscd(ct); cf=cf/cf.norm(dim=-1,keepdim=True)
            cvs=cf.cpu().numpy()
            hA=dict(h1)
            hB=dict(h1)
            for cv in cvs:
                rc=coll.search([cv.tolist()],"sscd_vec",P,15,output_fields=["image_pk"])
                for x in rc[0]:
                    k=x.entity.get("image_pk"); s=float(x.score)
                    hA[k]=max(hA.get(k,0),s)
                    hB[k]=max(hB.get(k,0),s)
            rv=coll.search([v.tolist()],"center_vec",P,15,output_fields=["image_pk"])
            for x in rv[0]:
                k=x.entity.get("image_pk"); s=float(x.score)
                hB[k]=max(hB.get(k,0),s)
            mA=[k for k,_ in sorted(hA.items(),key=lambda x:-x[1])[:10]]
            mB=[k for k,_ in sorted(hB.items(),key=lambda x:-x[1])[:10]]
        else:
            mA=list(h1.keys())[:10]; mB=mA
        if pk in mA: r2+=1
        if pk in mB: r3+=1
    n=len(files)
    print(f"{sc:<22} 单vec:{r1/n*100:>5.1f}%  单+S2:{r2/n*100:>5.1f}%  双+S2:{r3/n*100:>5.1f}%  差:{(r3-r2)/n*100:>+5.1f}%")
