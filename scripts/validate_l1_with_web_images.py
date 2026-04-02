#!/usr/bin/env python3
"""
L1 品类分类准确率验证 — 网络图片版

从 Unsplash/Pexels/Pixabay 下载已知品类的图片,
用 ViT-L-14 分类, 计算 ground-truth 准确率。

每个 L1 品类 ~10-15 张 × 18 类 ≈ 200+ 张有标注图片。

Usage:
    python3 scripts/validate_l1_with_web_images.py [--device cuda]
"""

import argparse
import io
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
import numpy as np
import torch
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "inference-service"))
from app.taxonomy import L1_CATEGORIES, L1_CODES, L1_CODE_TO_NAME

# ---------------------------------------------------------------------------
# 每个 L1 品类的测试图片 URL (ground truth)
# 来源: Unsplash (images.unsplash.com), Pexels, Pixabay
# ---------------------------------------------------------------------------

CATEGORY_IMAGES = {
    "服装类": [
        "https://images.unsplash.com/photo-1489987707025-afc232f7ea0f?w=400",  # shirts on rack
        "https://images.unsplash.com/photo-1434389677669-e08b4cda3a38?w=400",  # dress
        "https://images.unsplash.com/photo-1596755094514-f87e34085b2c?w=400",  # shirts
        "https://images.unsplash.com/photo-1551028719-00167b16eac5?w=400",  # jacket
        "https://images.unsplash.com/photo-1562157873-818bc0726f68?w=400",  # t-shirt
        "https://images.unsplash.com/photo-1594938298603-c8148c4dae35?w=400",  # sweater
        "https://images.unsplash.com/photo-1591047139829-d91aecb6caea?w=400",  # coat
        "https://images.unsplash.com/photo-1604176354204-9268737828e4?w=400",  # jeans
        "https://images.unsplash.com/photo-1558171813-4c088753af8f?w=400",  # hoodie
        "https://images.unsplash.com/photo-1583743814966-8936f5b7be1a?w=400",  # t-shirt plain
        "https://images.pexels.com/photos/1148957/pexels-photo-1148957.jpeg?w=400",  # clothes
        "https://images.pexels.com/photos/996329/pexels-photo-996329.jpeg?w=400",  # fashion
    ],
    "箱包类": [
        "https://images.unsplash.com/photo-1548036328-c9fa89d128fa?w=400",  # leather bag
        "https://images.unsplash.com/photo-1553062407-98eeb64c6a62?w=400",  # backpack
        "https://images.unsplash.com/photo-1590874103328-eac38a683ce7?w=400",  # handbag
        "https://images.unsplash.com/photo-1584917865442-de89df76afd3?w=400",  # purse
        "https://images.unsplash.com/photo-1547949003-9792a18a2601?w=400",  # bags
        "https://images.unsplash.com/photo-1575844264771-892081089af5?w=400",  # suitcase
        "https://images.unsplash.com/photo-1622560480654-d96214fdc887?w=400",  # wallet
        "https://images.unsplash.com/photo-1559563458-527698bf5295?w=400",  # travel bag
        "https://images.pexels.com/photos/1152077/pexels-photo-1152077.jpeg?w=400",  # handbag
        "https://images.pexels.com/photos/904350/pexels-photo-904350.jpeg?w=400",  # bag
    ],
    "鞋帽类": [
        "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=400",  # sneaker
        "https://images.unsplash.com/photo-1460353581641-37baddab0fa2?w=400",  # running shoes
        "https://images.unsplash.com/photo-1543163521-1bf539c55dd2?w=400",  # sneakers pair
        "https://images.unsplash.com/photo-1595950653106-6c9ebd614d3a?w=400",  # colorful shoes
        "https://images.unsplash.com/photo-1606107557195-0e29a4b5b4aa?w=400",  # white sneakers
        "https://images.unsplash.com/photo-1549298916-b41d501d3772?w=400",  # shoes
        "https://images.unsplash.com/photo-1587563871167-1ee9c731aefb?w=400",  # high heels
        "https://images.unsplash.com/photo-1608231387042-66d1773070a5?w=400",  # boots
        "https://images.unsplash.com/photo-1588850561407-ed78c334e67a?w=400",  # cap hat
        "https://images.pexels.com/photos/2529148/pexels-photo-2529148.jpeg?w=400",  # sneakers
        "https://images.pexels.com/photos/1598505/pexels-photo-1598505.jpeg?w=400",  # shoes
        "https://cdn.pixabay.com/photo/2016/11/19/18/06/feet-1840619_640.jpg",  # shoes
    ],
    "珠宝首饰类": [
        "https://images.unsplash.com/photo-1515562141589-67f0d94de39d?w=400",  # rings
        "https://images.unsplash.com/photo-1573408301185-9146fe634ad0?w=400",  # jewelry
        "https://images.unsplash.com/photo-1611591437281-460bfbe1220a?w=400",  # necklace
        "https://images.unsplash.com/photo-1535632066927-ab7c9ab60908?w=400",  # earrings
        "https://images.unsplash.com/photo-1605100804763-247f67b3557e?w=400",  # bracelet
        "https://images.unsplash.com/photo-1602751584552-8ba73aad10e1?w=400",  # diamond ring
        "https://images.unsplash.com/photo-1599643478518-a784e5dc4c8f?w=400",  # gold jewelry
        "https://images.unsplash.com/photo-1603561596112-0a132b757442?w=400",  # necklace pendant
        "https://images.pexels.com/photos/1191531/pexels-photo-1191531.jpeg?w=400",  # jewelry
        "https://images.pexels.com/photos/248077/pexels-photo-248077.jpeg?w=400",  # rings
    ],
    "房地产类": [
        "https://images.unsplash.com/photo-1564013799919-ab600027ffc6?w=400",  # house
        "https://images.unsplash.com/photo-1570129477492-45c003edd2be?w=400",  # home
        "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=400",  # villa
        "https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=400",  # house exterior
        "https://images.unsplash.com/photo-1512917774080-9991f1c4c750?w=400",  # modern house
        "https://images.unsplash.com/photo-1600607687939-ce8a6c25118c?w=400",  # interior
        "https://images.unsplash.com/photo-1600566753190-17f0baa2a6c3?w=400",  # apartment
        "https://images.unsplash.com/photo-1560448204-e02f11c3d0e2?w=400",  # living room
        "https://images.pexels.com/photos/106399/pexels-photo-106399.jpeg?w=400",  # house
        "https://images.pexels.com/photos/323780/pexels-photo-323780.jpeg?w=400",  # building
    ],
    "五金建材类": [
        "https://images.unsplash.com/photo-1504148455328-c376907d081c?w=400",  # tools
        "https://images.unsplash.com/photo-1581783898377-1c85bf937427?w=400",  # power drill
        "https://images.unsplash.com/photo-1572981779307-38b8cabb2407?w=400",  # tools workshop
        "https://images.unsplash.com/photo-1530124566582-a45a7e3d0d73?w=400",  # wrench
        "https://images.unsplash.com/photo-1617791160536-598cf32026fb?w=400",  # screwdriver
        "https://images.unsplash.com/photo-1558618666-fcd25c85f82e?w=400",  # measuring tape
        "https://images.pexels.com/photos/162553/keys-workshop-mechanic-tools-162553.jpeg?w=400",
        "https://images.pexels.com/photos/1249611/pexels-photo-1249611.jpeg?w=400",  # hardware
        "https://images.pexels.com/photos/4491881/pexels-photo-4491881.jpeg?w=400",  # tools
        "https://cdn.pixabay.com/photo/2015/12/07/10/55/drill-1080590_640.jpg",  # drill
    ],
    "家具类": [
        "https://images.unsplash.com/photo-1555041469-a586c61ea9bc?w=400",  # sofa
        "https://images.unsplash.com/photo-1506439773649-6e0eb8cfb237?w=400",  # chair
        "https://images.unsplash.com/photo-1524758631624-e2822e304c36?w=400",  # desk
        "https://images.unsplash.com/photo-1493663284031-b7e3aefcae8e?w=400",  # living room
        "https://images.unsplash.com/photo-1550581190-9c1c48d21d6c?w=400",  # bookshelf
        "https://images.unsplash.com/photo-1540574163026-643ea20ade25?w=400",  # bed
        "https://images.unsplash.com/photo-1594026112284-02bb6f3352fe?w=400",  # table
        "https://images.unsplash.com/photo-1538688525198-9b88f6f53126?w=400",  # cabinet
        "https://images.pexels.com/photos/1350789/pexels-photo-1350789.jpeg?w=400",  # furniture
        "https://images.pexels.com/photos/276583/pexels-photo-276583.jpeg?w=400",  # sofa
    ],
    "化妆品类": [
        "https://images.unsplash.com/photo-1596462502278-27bfdc403348?w=400",  # makeup
        "https://images.unsplash.com/photo-1522335789203-aabd1fc54bc9?w=400",  # cosmetics
        "https://images.unsplash.com/photo-1586495777744-4413f21062fa?w=400",  # lipstick
        "https://images.unsplash.com/photo-1571781926291-c477ebfd024b?w=400",  # skincare
        "https://images.unsplash.com/photo-1541643600914-78b084683601?w=400",  # perfume
        "https://images.unsplash.com/photo-1612817288484-6f916006741a?w=400",  # skincare bottles
        "https://images.unsplash.com/photo-1631729371254-42c2892f0e6e?w=400",  # cosmetics set
        "https://images.pexels.com/photos/2587370/pexels-photo-2587370.jpeg?w=400",  # makeup
        "https://images.pexels.com/photos/3373736/pexels-photo-3373736.jpeg?w=400",  # skincare
        "https://images.pexels.com/photos/2253833/pexels-photo-2253833.jpeg?w=400",  # perfume
    ],
    "小家电类": [
        "https://images.unsplash.com/photo-1585659722983-3a675dabf23d?w=400",  # hair dryer
        "https://images.unsplash.com/photo-1570222094114-d054a817e56b?w=400",  # coffee maker
        "https://images.unsplash.com/photo-1556909114-f6e7ad7d3136?w=400",  # blender
        "https://images.unsplash.com/photo-1544568100-847a948585b9?w=400",  # toaster
        "https://images.unsplash.com/photo-1585515320310-259814833e62?w=400",  # iron
        "https://images.unsplash.com/photo-1622480914645-fafbe3e13c7b?w=400",  # air fryer
        "https://images.pexels.com/photos/1493226/pexels-photo-1493226.jpeg?w=400",  # vacuum
        "https://images.pexels.com/photos/2081832/pexels-photo-2081832.jpeg?w=400",  # coffee machine
        "https://images.pexels.com/photos/4108715/pexels-photo-4108715.jpeg?w=400",  # blender
        "https://cdn.pixabay.com/photo/2020/02/09/12/09/coffee-machine-4832831_640.jpg",
    ],
    "手机类": [
        "https://images.unsplash.com/photo-1511707171634-5f897ff02aa9?w=400",  # phone
        "https://images.unsplash.com/photo-1592899677977-9c10ca588bbd?w=400",  # iphone
        "https://images.unsplash.com/photo-1601784551446-20c9e07cdbdb?w=400",  # smartphone
        "https://images.unsplash.com/photo-1565849904461-04a58ad377e0?w=400",  # phone
        "https://images.unsplash.com/photo-1605236453806-6ff36851218e?w=400",  # iphone
        "https://images.unsplash.com/photo-1580910051074-3eb694886f4b?w=400",  # phone case
        "https://images.pexels.com/photos/699122/pexels-photo-699122.jpeg?w=400",  # phone
        "https://images.pexels.com/photos/788946/pexels-photo-788946.jpeg?w=400",  # phones
        "https://images.pexels.com/photos/607812/pexels-photo-607812.jpeg?w=400",  # iphone
        "https://images.pexels.com/photos/1092644/pexels-photo-1092644.jpeg?w=400",  # phone
    ],
    "电脑类": [
        "https://images.unsplash.com/photo-1496181133206-80ce9b88a853?w=400",  # laptop
        "https://images.unsplash.com/photo-1517694712202-14dd9538aa97?w=400",  # laptop code
        "https://images.unsplash.com/photo-1593642632559-0c6d3fc62b89?w=400",  # macbook
        "https://images.unsplash.com/photo-1527443224154-c4a3942d3acf?w=400",  # monitor
        "https://images.unsplash.com/photo-1587831990711-23ca6441447b?w=400",  # keyboard
        "https://images.unsplash.com/photo-1547082299-de196ea013d6?w=400",  # desktop setup
        "https://images.pexels.com/photos/18105/pexels-photo.jpg?w=400",  # macbook
        "https://images.pexels.com/photos/1229861/pexels-photo-1229861.jpeg?w=400",  # laptop
        "https://images.pexels.com/photos/7974/pexels-photo.jpg?w=400",  # workspace
        "https://cdn.pixabay.com/photo/2015/02/02/11/09/office-620822_640.jpg",  # computer
    ],
    "食品类": [
        "https://images.unsplash.com/photo-1621939514649-280e2ee25f60?w=400",  # snacks
        "https://images.unsplash.com/photo-1568901346375-23c9450c58cd?w=400",  # burger
        "https://images.unsplash.com/photo-1599599810694-b5b37304c041?w=400",  # chocolate
        "https://images.unsplash.com/photo-1567306226416-28f0efdc88ce?w=400",  # fruits
        "https://images.unsplash.com/photo-1606312619070-d48b4c652a52?w=400",  # wine bottles
        "https://images.unsplash.com/photo-1558160074-4d7d8bdf4256?w=400",  # coffee beans
        "https://images.pexels.com/photos/1640777/pexels-photo-1640777.jpeg?w=400",  # food
        "https://images.pexels.com/photos/376464/pexels-photo-376464.jpeg?w=400",  # pancakes
        "https://images.pexels.com/photos/1099680/pexels-photo-1099680.jpeg?w=400",  # food
        "https://images.pexels.com/photos/2983101/pexels-photo-2983101.jpeg?w=400",  # cookies
    ],
    "玩具类": [
        "https://images.unsplash.com/photo-1558060370-d644479cb6f7?w=400",  # lego
        "https://images.unsplash.com/photo-1566576912321-d58ddd7a6088?w=400",  # teddy bear
        "https://images.unsplash.com/photo-1596461404969-9ae70f2830c1?w=400",  # toys
        "https://images.unsplash.com/photo-1581235707960-e2e92d22c829?w=400",  # action figure
        "https://images.unsplash.com/photo-1608889825205-eebdb9fc5806?w=400",  # toy car
        "https://images.pexels.com/photos/163036/mario-luigi-yoschi-figures-163036.jpeg?w=400",
        "https://images.pexels.com/photos/1679618/pexels-photo-1679618.jpeg?w=400",  # puzzle
        "https://images.pexels.com/photos/3661244/pexels-photo-3661244.jpeg?w=400",  # toys
        "https://images.pexels.com/photos/168866/pexels-photo-168866.jpeg?w=400",  # building blocks
        "https://cdn.pixabay.com/photo/2018/04/26/12/14/travel-3351825_640.jpg",  # toy figure
    ],
    "运动户外类": [
        "https://images.unsplash.com/photo-1461896836934-bd45ba7b9a20?w=400",  # bicycle
        "https://images.unsplash.com/photo-1571019614242-c5c5dee9f50b?w=400",  # gym
        "https://images.unsplash.com/photo-1593079831268-3381b0db4a77?w=400",  # dumbbell
        "https://images.unsplash.com/photo-1554284126-aa88f22d8b74?w=400",  # yoga mat
        "https://images.unsplash.com/photo-1504609773096-104ff2c73ba4?w=400",  # camping
        "https://images.unsplash.com/photo-1529490831155-738c777075a2?w=400",  # tent
        "https://images.pexels.com/photos/3755440/pexels-photo-3755440.jpeg?w=400",  # fitness
        "https://images.pexels.com/photos/46798/the-ball-stadion-football-the-pitch-46798.jpeg?w=400",
        "https://images.pexels.com/photos/4761352/pexels-photo-4761352.jpeg?w=400",  # cycling
        "https://cdn.pixabay.com/photo/2017/01/13/04/56/basketball-1976538_640.jpg",
    ],
    "汽车配件类": [
        "https://images.unsplash.com/photo-1486262715619-67b85e0b08d3?w=400",  # car wheel
        "https://images.unsplash.com/photo-1558618666-fcd25c85f82e?w=400",  # car parts
        "https://images.unsplash.com/photo-1619642751034-765dfdf7c58e?w=400",  # car seat
        "https://images.unsplash.com/photo-1503736334956-4c8f8e92946d?w=400",  # car dashboard
        "https://images.unsplash.com/photo-1549317661-bd32c8ce0afe?w=400",  # car light
        "https://images.pexels.com/photos/3807517/pexels-photo-3807517.jpeg?w=400",  # car interior
        "https://images.pexels.com/photos/3757226/pexels-photo-3757226.jpeg?w=400",  # tire
        "https://images.pexels.com/photos/2244746/pexels-photo-2244746.jpeg?w=400",  # car detail
        "https://cdn.pixabay.com/photo/2019/04/09/21/33/seat-4115687_640.jpg",  # car seat
        "https://cdn.pixabay.com/photo/2016/11/18/15/03/auto-1835506_640.jpg",  # dashboard
    ],
    "办公用品类": [
        "https://images.unsplash.com/photo-1583485088034-697b5bc54ccd?w=400",  # pens
        "https://images.unsplash.com/photo-1586281380349-632531db7ed4?w=400",  # notebook
        "https://images.unsplash.com/photo-1612815154858-60aa4c59eaa6?w=400",  # office supplies
        "https://images.unsplash.com/photo-1456735190827-d1262f71b8a3?w=400",  # stationery
        "https://images.unsplash.com/photo-1612287230202-1ff1d85d1bdf?w=400",  # printer
        "https://images.pexels.com/photos/159618/pexels-photo-159618.jpeg?w=400",  # scissors
        "https://images.pexels.com/photos/1765033/pexels-photo-1765033.jpeg?w=400",  # pen
        "https://images.pexels.com/photos/6476805/pexels-photo-6476805.jpeg?w=400",  # stationery
        "https://cdn.pixabay.com/photo/2015/01/21/14/14/pencils-606736_640.jpg",  # pencils
        "https://cdn.pixabay.com/photo/2016/11/23/14/49/book-1853677_640.jpg",  # notebook
    ],
    "钟表类": [
        "https://images.unsplash.com/photo-1523170335258-f5ed11844a49?w=400",  # watch
        "https://images.unsplash.com/photo-1524592094714-0f0654e20314?w=400",  # pocket watch
        "https://images.unsplash.com/photo-1542496658-e33a6d0d50f6?w=400",  # wristwatch
        "https://images.unsplash.com/photo-1509048191080-d2984bad6ae5?w=400",  # luxury watch
        "https://images.unsplash.com/photo-1539874754764-5a96559165b0?w=400",  # watch face
        "https://images.unsplash.com/photo-1522312346375-d1a52e2b99b3?w=400",  # watch
        "https://images.pexels.com/photos/190819/pexels-photo-190819.jpeg?w=400",  # watch
        "https://images.pexels.com/photos/2113994/pexels-photo-2113994.jpeg?w=400",  # smartwatch
        "https://images.pexels.com/photos/125779/pexels-photo-125779.jpeg?w=400",  # watch
        "https://cdn.pixabay.com/photo/2014/07/31/23/00/wristwatch-407096_640.jpg",
    ],
    "眼镜类": [
        "https://images.unsplash.com/photo-1572635196237-14b3f281503f?w=400",  # sunglasses
        "https://images.unsplash.com/photo-1511499767150-a48a237f0083?w=400",  # sunglasses
        "https://images.unsplash.com/photo-1574258495973-f010dfbb5371?w=400",  # glasses
        "https://images.unsplash.com/photo-1577803645773-f96470509666?w=400",  # eyeglasses
        "https://images.unsplash.com/photo-1509695507497-903c140c43b0?w=400",  # reading glasses
        "https://images.pexels.com/photos/701877/pexels-photo-701877.jpeg?w=400",  # sunglasses
        "https://images.pexels.com/photos/975668/pexels-photo-975668.jpeg?w=400",  # glasses
        "https://images.pexels.com/photos/947885/pexels-photo-947885.jpeg?w=400",  # eyewear
        "https://cdn.pixabay.com/photo/2017/07/31/22/38/spectacles-2561221_640.jpg",
        "https://cdn.pixabay.com/photo/2016/04/01/10/55/sunglasses-1299088_640.jpg",
    ],
}


def download_images(category_images: dict, timeout=15):
    """下载所有品类图片"""
    client = httpx.Client(timeout=timeout, follow_redirects=True, headers={
        "User-Agent": "Mozilla/5.0 (compatible; ImageTest/1.0)"
    })

    all_images = []
    total_urls = sum(len(urls) for urls in category_images.values())
    downloaded = 0
    failed = 0

    for category, urls in category_images.items():
        cat_ok = 0
        for url in urls:
            try:
                resp = client.get(url)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    img = Image.open(io.BytesIO(resp.content)).convert("RGB")
                    all_images.append({
                        "image": img,
                        "expected_l1": category,
                        "url": url,
                    })
                    cat_ok += 1
                    downloaded += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1

        print(f"  {category}: {cat_ok}/{len(urls)} downloaded")

    client.close()
    print(f"  总计: {downloaded} 成功, {failed} 失败 (共 {total_urls} URL)")
    return all_images


@torch.no_grad()
def classify_images(model, preprocess, text_features, l1_names, images, device):
    """分类所有图片"""
    results = []
    batch_size = 32

    for i in range(0, len(images), batch_size):
        batch = images[i:i + batch_size]
        tensors = []
        for item in batch:
            tensors.append(preprocess(item["image"]))

        batch_t = torch.stack(tensors).to(device)
        features = model.encode_image(batch_t)
        features = features / features.norm(dim=-1, keepdim=True)
        sims = (features.float() @ text_features.T)

        for k, item in enumerate(batch):
            idx = sims[k].argmax().item()
            conf = sims[k][idx].item()
            top3_vals, top3_idx = sims[k].topk(3)
            top3 = [(l1_names[ti], round(tv.item(), 4)) for ti, tv in zip(top3_idx, top3_vals)]

            results.append({
                "expected_l1": item["expected_l1"],
                "predicted_l1": l1_names[idx],
                "confidence": round(conf, 4),
                "correct": item["expected_l1"] == l1_names[idx],
                "top3": top3,
                "url": item["url"],
            })

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", default="cuda")
    args = parser.parse_args()

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] L1 品类分类准确率验证 (网络图片)")
    print(f"  品类数: {len(CATEGORY_IMAGES)}, 设备: {args.device}")
    print()

    # 1. 下载图片
    print("[1/3] 下载测试图片...")
    t0 = time.time()
    images = download_images(CATEGORY_IMAGES)
    print(f"  下载耗时: {time.time()-t0:.1f}s\n")

    if not images:
        print("ERROR: No images downloaded!")
        return

    # 2. 加载模型
    print("[2/3] 加载 ViT-L-14...")
    import open_clip
    _orig = torch.load
    torch.load = lambda *a, **kw: _orig(*a, **{**kw, "weights_only": False})
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-L-14", pretrained="/data/imgsrch/models/ViT-L-14.pt"
    )
    torch.load = _orig
    model.eval().to(args.device)
    tokenizer = open_clip.get_tokenizer("ViT-L-14")

    # L1 文本特征
    l1_names = [L1_CATEGORIES[c]["name"] for c in L1_CODES]
    all_texts, counts = [], []
    for code in L1_CODES:
        prompts = L1_CATEGORIES[code]["prompts"]
        all_texts.extend(prompts)
        counts.append(len(prompts))
    tokens = tokenizer(all_texts).to(args.device)
    with torch.no_grad():
        feats = model.encode_text(tokens)
    feats = feats / feats.norm(dim=-1, keepdim=True)
    averaged = []
    idx = 0
    for c in counts:
        avg = feats[idx:idx+c].mean(dim=0)
        avg = avg / avg.norm()
        averaged.append(avg)
        idx += c
    text_features = torch.stack(averaged).float().to(args.device)
    print(f"  模型就绪, {len(l1_names)} 品类\n")

    # 3. 分类
    print(f"[3/3] 分类 {len(images)} 张图片...")
    t1 = time.time()
    results = classify_images(model, preprocess, text_features, l1_names, images, args.device)
    elapsed = time.time() - t1
    print(f"  分类完成: {elapsed:.1f}s\n")

    # 4. 统计准确率
    total = len(results)
    correct = sum(1 for r in results if r["correct"])
    accuracy = correct / total * 100

    # Top-3 准确率
    top3_correct = sum(1 for r in results if r["expected_l1"] in [t[0] for t in r["top3"]])
    top3_accuracy = top3_correct / total * 100

    print("=" * 70)
    print(f"总体准确率: {correct}/{total} = {accuracy:.1f}%")
    print(f"Top-3 准确率: {top3_correct}/{total} = {top3_accuracy:.1f}%")
    print("=" * 70)

    # 按品类统计
    cat_stats = defaultdict(lambda: {"total": 0, "correct": 0, "wrong_as": defaultdict(int)})
    for r in results:
        cat = r["expected_l1"]
        cat_stats[cat]["total"] += 1
        if r["correct"]:
            cat_stats[cat]["correct"] += 1
        else:
            cat_stats[cat]["wrong_as"][r["predicted_l1"]] += 1

    print(f"\n{'品类':<12} {'正确':>4} {'总数':>4} {'准确率':>7} {'主要误分':>20}")
    print("-" * 55)
    for cat in CATEGORY_IMAGES.keys():
        s = cat_stats[cat]
        acc = s["correct"] / s["total"] * 100 if s["total"] > 0 else 0
        wrong = ""
        if s["wrong_as"]:
            top_wrong = max(s["wrong_as"].items(), key=lambda x: x[1])
            wrong = f"→ {top_wrong[0]}({top_wrong[1]})"
        print(f"{cat:<12} {s['correct']:>4} {s['total']:>4} {acc:>6.1f}% {wrong:>20}")

    # 详细错误
    wrong_results = [r for r in results if not r["correct"]]
    if wrong_results:
        print(f"\n{'='*70}")
        print(f"错误详情 ({len(wrong_results)} 条)")
        print("=" * 70)
        for r in wrong_results:
            print(f"  期望: {r['expected_l1']:<10} 预测: {r['predicted_l1']:<10} "
                  f"conf={r['confidence']:.4f}  top3={[t[0] for t in r['top3']]}")

    # 保存结果
    output_dir = "/data/imgsrch/task_logs"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"l1_web_validation_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(output_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "total": total, "correct": correct,
            "accuracy": round(accuracy, 2),
            "top3_accuracy": round(top3_accuracy, 2),
            "per_category": {cat: {"correct": s["correct"], "total": s["total"],
                                   "accuracy": round(s["correct"]/s["total"]*100, 1) if s["total"] > 0 else 0}
                             for cat, s in cat_stats.items()},
            "errors": wrong_results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果保存: {output_file}")


if __name__ == "__main__":
    main()
