"""
taxonomy.py — 两级分类品类体系定义

Stage 1: ViT-L-14 通用模型 → L1 大类 (18类, 95%+准确率)
Stage 2: FashionSigLIP (时尚类 L2/L3) + ViT-L-14 零样本 (其他类 L2/L3)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set

# ---------------------------------------------------------------------------
# L1 大类定义
# ---------------------------------------------------------------------------

FASHION_L1_CODES: Set[int] = {1001, 1002, 1003}  # 路由到 FashionSigLIP

L1_CATEGORIES: Dict[int, dict] = {
    1001: {
        "name": "服装类",
        "prompts": [
            "a product photo of clothing",
            "a photo of garment apparel fashion wear",
            "a commercial photo of shirt pants dress jacket coat",
        ],
    },
    1002: {
        "name": "箱包类",
        "prompts": [
            "a product photo of bag luggage",
            "a photo of handbag purse backpack suitcase briefcase",
            "a commercial photo of leather bag travel luggage",
        ],
    },
    1003: {
        "name": "鞋帽类",
        "prompts": [
            "a product photo of shoes footwear hat cap",
            "a photo of sneakers boots heels sandals loafers",
            "a commercial photo of athletic shoes fashion hat beanie",
        ],
    },
    1004: {
        "name": "珠宝首饰类",
        "prompts": [
            "a product photo of jewelry accessories",
            "a photo of ring necklace bracelet earring brooch",
            "a commercial photo of gold diamond jade gem jewel",
        ],
    },
    1005: {
        "name": "房地产类",
        "prompts": [
            "a photo of real estate property building",
            "a photo of house apartment villa interior exterior",
            "a commercial photo of residential commercial architecture",
        ],
    },
    1006: {
        "name": "五金建材类",
        "prompts": [
            "a product photo of hardware building materials",
            "a photo of tools screws bolts pipes fittings",
            "a commercial photo of construction materials plumbing electrical",
        ],
    },
    1007: {
        "name": "家具类",
        "prompts": [
            "a product photo of furniture home furnishing",
            "a photo of sofa table chair cabinet bed desk shelf",
            "a commercial photo of modern furniture interior decor",
        ],
    },
    1008: {
        "name": "化妆品类",
        "prompts": [
            "a product photo of cosmetics beauty skincare",
            "a photo of lipstick foundation perfume cream serum",
            "a commercial photo of makeup beauty product skincare",
        ],
    },
    1009: {
        "name": "小家电类",
        "prompts": [
            "a product photo of small home appliance",
            "a photo of hair dryer blender toaster coffee maker iron",
            "a commercial photo of kitchen appliance personal care device",
        ],
    },
    1010: {
        "name": "手机类",
        "prompts": [
            "a product photo of smartphone mobile phone",
            "a photo of cellphone phone screen device",
            "a commercial photo of mobile phone smartphone device",
        ],
    },
    1011: {
        "name": "电脑类",
        "prompts": [
            "a product photo of computer laptop desktop",
            "a photo of PC laptop monitor keyboard mouse tablet",
            "a commercial photo of notebook computer workstation",
        ],
    },
    1012: {
        "name": "食品类",
        "prompts": [
            "a product photo of food snack beverage",
            "a photo of packaged food drink grocery",
            "a commercial photo of gourmet food product packaging",
        ],
    },
    1013: {
        "name": "玩具类",
        "prompts": [
            "a product photo of toy game",
            "a photo of children toy doll action figure puzzle",
            "a commercial photo of toy game collectible figure",
        ],
    },
    1014: {
        "name": "运动户外类",
        "prompts": [
            "a product photo of sports outdoor equipment",
            "a photo of fitness gear camping hiking cycling equipment",
            "a commercial photo of athletic sports outdoor product",
        ],
    },
    1015: {
        "name": "汽车配件类",
        "prompts": [
            "a product photo of auto parts accessories",
            "a photo of car accessories vehicle parts component",
            "a commercial photo of automotive accessory part",
        ],
    },
    1016: {
        "name": "办公用品类",
        "prompts": [
            "a product photo of office supplies stationery",
            "a photo of pen notebook printer paper organizer",
            "a commercial photo of office product stationery supply",
        ],
    },
    1017: {
        "name": "钟表类",
        "prompts": [
            "a product photo of watch clock timepiece",
            "a photo of wristwatch luxury watch wall clock",
            "a commercial photo of fine watch chronograph timepiece",
        ],
    },
    1018: {
        "name": "眼镜类",
        "prompts": [
            "a product photo of eyewear glasses",
            "a photo of sunglasses eyeglasses optical frame",
            "a commercial photo of designer glasses spectacles",
        ],
    },
}

# 快速查找: code → name, name → code
L1_CODE_TO_NAME: Dict[int, str] = {c: v["name"] for c, v in L1_CATEGORIES.items()}
L1_NAME_TO_CODE: Dict[str, int] = {v["name"]: c for c, v in L1_CATEGORIES.items()}
L1_NAMES: List[str] = [L1_CATEGORIES[c]["name"] for c in sorted(L1_CATEGORIES)]
L1_CODES: List[int] = sorted(L1_CATEGORIES.keys())

# ---------------------------------------------------------------------------
# L2 子品类 — 时尚类由 FashionSigLIP 处理, 非时尚类由 ViT-L-14 零样本处理
# ---------------------------------------------------------------------------

# --- 时尚类 L2 (FashionSigLIP 空间的 prompt) ---
FASHION_L2: Dict[int, Dict[int, dict]] = {
    # 服装类 L2
    1001: {
        100101: {"name": "上装", "prompts": ["a top shirt blouse tee sweater hoodie"]},
        100102: {"name": "下装", "prompts": ["pants trousers jeans shorts leggings"]},
        100103: {"name": "连衣裙", "prompts": ["a dress gown one-piece frock"]},
        100104: {"name": "外套", "prompts": ["a jacket coat outerwear blazer parka"]},
        100105: {"name": "内衣", "prompts": ["underwear lingerie bra nightwear sleepwear"]},
        100106: {"name": "运动服", "prompts": ["sportswear athletic wear activewear tracksuit"]},
        100107: {"name": "套装", "prompts": ["a matching set suit outfit co-ord"]},
    },
    # 箱包类 L2
    1002: {
        100201: {"name": "手提包", "prompts": ["a handbag tote bag purse"]},
        100202: {"name": "双肩包", "prompts": ["a backpack rucksack school bag"]},
        100203: {"name": "单肩包", "prompts": ["a shoulder bag crossbody bag messenger bag"]},
        100204: {"name": "钱包", "prompts": ["a wallet purse card holder billfold"]},
        100205: {"name": "旅行箱", "prompts": ["luggage suitcase travel bag trolley"]},
        100206: {"name": "腰包", "prompts": ["a belt bag fanny pack waist bag"]},
    },
    # 鞋帽类 L2
    1003: {
        100301: {"name": "运动鞋", "prompts": ["sneakers running shoes athletic shoes trainers"]},
        100302: {"name": "皮鞋", "prompts": ["leather shoes oxford derby loafer"]},
        100303: {"name": "高跟鞋", "prompts": ["high heels pumps stilettos"]},
        100304: {"name": "靴子", "prompts": ["boots ankle boots knee boots chelsea boots"]},
        100305: {"name": "凉鞋拖鞋", "prompts": ["sandals slippers flip flops slides"]},
        100306: {"name": "帽子", "prompts": ["a hat cap beanie beret baseball cap"]},
    },
}

# --- 时尚类 L3 (FashionSigLIP 空间的 prompt, 按 L2 分组) ---
FASHION_L3: Dict[int, Dict[int, dict]] = {
    # 上装 L3
    100101: {
        10010101: {"name": "T恤", "prompts": ["a t-shirt tee crew neck v-neck"]},
        10010102: {"name": "衬衫", "prompts": ["a dress shirt button-up oxford shirt"]},
        10010103: {"name": "卫衣", "prompts": ["a hoodie sweatshirt pullover"]},
        10010104: {"name": "毛衣", "prompts": ["a sweater knit jumper cardigan pullover"]},
        10010105: {"name": "Polo衫", "prompts": ["a polo shirt collared knit shirt"]},
        10010106: {"name": "背心吊带", "prompts": ["a tank top camisole vest spaghetti strap"]},
        10010107: {"name": "马甲", "prompts": ["a vest waistcoat gilet sleeveless jacket"]},
    },
    # 下装 L3
    100102: {
        10010201: {"name": "牛仔裤", "prompts": ["jeans denim pants"]},
        10010202: {"name": "休闲裤", "prompts": ["casual pants chinos khakis trousers"]},
        10010203: {"name": "短裤", "prompts": ["shorts bermuda shorts hot pants"]},
        10010204: {"name": "运动裤", "prompts": ["sweatpants joggers track pants"]},
        10010205: {"name": "半身裙", "prompts": ["a skirt mini skirt midi skirt pencil skirt"]},
        10010206: {"name": "打底裤", "prompts": ["leggings tights yoga pants"]},
    },
    # 连衣裙 L3
    100103: {
        10010301: {"name": "休闲连衣裙", "prompts": ["a casual dress day dress cotton dress"]},
        10010302: {"name": "正式连衣裙", "prompts": ["a formal dress evening gown cocktail dress"]},
        10010303: {"name": "衬衫裙", "prompts": ["a shirt dress button front dress"]},
        10010304: {"name": "针织裙", "prompts": ["a knit dress sweater dress bodycon"]},
        10010305: {"name": "吊带裙", "prompts": ["a slip dress spaghetti strap dress"]},
    },
    # 外套 L3
    100104: {
        10010401: {"name": "羽绒服", "prompts": ["a down jacket puffer coat quilted jacket"]},
        10010402: {"name": "风衣", "prompts": ["a trench coat windbreaker raincoat"]},
        10010403: {"name": "西装外套", "prompts": ["a blazer suit jacket sport coat"]},
        10010404: {"name": "皮夹克", "prompts": ["a leather jacket biker jacket moto jacket"]},
        10010405: {"name": "牛仔外套", "prompts": ["a denim jacket jean jacket trucker jacket"]},
        10010406: {"name": "大衣", "prompts": ["an overcoat wool coat long coat topcoat"]},
    },
    # 手提包 L3
    100201: {
        10020101: {"name": "托特包", "prompts": ["a tote bag large open top bag"]},
        10020102: {"name": "迷你手提包", "prompts": ["a mini bag small handbag micro bag"]},
        10020103: {"name": "公文包", "prompts": ["a briefcase business bag document bag"]},
    },
    # 运动鞋 L3
    100301: {
        10030101: {"name": "跑步鞋", "prompts": ["running shoes jogging shoes marathon shoes"]},
        10030102: {"name": "板鞋", "prompts": ["skate shoes flat sneakers casual sneakers"]},
        10030103: {"name": "篮球鞋", "prompts": ["basketball shoes high top sneakers"]},
        10030104: {"name": "老爹鞋", "prompts": ["chunky sneakers dad shoes platform sneakers"]},
    },
}

# ---------------------------------------------------------------------------
# 非时尚类 L2 (ViT-L-14 零样本, 英文 prompt)
# ---------------------------------------------------------------------------

NONFASHION_L2: Dict[int, Dict[int, dict]] = {
    # 珠宝首饰类 L2
    1004: {
        100401: {"name": "戒指", "prompts": ["a photo of a ring, diamond ring, gold ring, engagement ring"]},
        100402: {"name": "项链", "prompts": ["a photo of a necklace, pendant, chain necklace"]},
        100403: {"name": "手镯手链", "prompts": ["a photo of a bracelet, bangle, wrist jewelry"]},
        100404: {"name": "耳环耳饰", "prompts": ["a photo of earrings, stud earrings, hoop earrings, drop earrings"]},
        100405: {"name": "翡翠玉器", "prompts": ["a photo of jade jewelry, jadeite, nephrite, green jade"]},
        100406: {"name": "黄金饰品", "prompts": ["a photo of gold jewelry, gold bar, gold chain"]},
        100407: {"name": "钻石宝石", "prompts": ["a photo of diamond, gemstone, ruby, sapphire, emerald"]},
        100408: {"name": "胸针别针", "prompts": ["a photo of a brooch, pin, lapel pin"]},
    },
    # 房地产类 L2
    1005: {
        100501: {"name": "住宅", "prompts": ["a photo of residential property, house, apartment, condo"]},
        100502: {"name": "商铺", "prompts": ["a photo of commercial property, shop, retail store, storefront"]},
        100503: {"name": "室内装修", "prompts": ["a photo of interior design, home decoration, room renovation"]},
        100504: {"name": "户型图", "prompts": ["a floor plan, layout diagram, apartment blueprint"]},
    },
    # 五金建材类 L2
    1006: {
        100601: {"name": "水暖管件", "prompts": ["a photo of plumbing fittings, pipes, valves, faucets"]},
        100602: {"name": "电气配件", "prompts": ["a photo of electrical fittings, switches, sockets, wires"]},
        100603: {"name": "工具", "prompts": ["a photo of hand tools, power tools, wrench, drill, hammer"]},
        100604: {"name": "装饰建材", "prompts": ["a photo of tiles, paint, wallpaper, flooring materials"]},
        100605: {"name": "门窗五金", "prompts": ["a photo of door hardware, window fittings, locks, hinges"]},
    },
    # 家具类 L2
    1007: {
        100701: {"name": "沙发", "prompts": ["a photo of sofa, couch, loveseat, sectional"]},
        100702: {"name": "桌子", "prompts": ["a photo of table, desk, dining table, coffee table"]},
        100703: {"name": "椅子", "prompts": ["a photo of chair, armchair, office chair, stool"]},
        100704: {"name": "柜子", "prompts": ["a photo of cabinet, wardrobe, dresser, shelf, bookcase"]},
        100705: {"name": "床", "prompts": ["a photo of bed, bedframe, bunk bed, mattress"]},
        100706: {"name": "灯具", "prompts": ["a photo of lamp, chandelier, pendant light, floor lamp"]},
    },
    # 化妆品类 L2
    1008: {
        100801: {"name": "彩妆", "prompts": ["a photo of makeup, lipstick, eyeshadow, blush, foundation"]},
        100802: {"name": "护肤", "prompts": ["a photo of skincare, moisturizer, serum, cleanser, toner"]},
        100803: {"name": "香水", "prompts": ["a photo of perfume, fragrance, cologne, eau de toilette"]},
        100804: {"name": "美容工具", "prompts": ["a photo of beauty tool, makeup brush, mirror, sponge"]},
        100805: {"name": "身体护理", "prompts": ["a photo of body care, lotion, shower gel, sunscreen"]},
    },
    # 小家电类 L2
    1009: {
        100901: {"name": "厨房小家电", "prompts": ["a photo of kitchen appliance, blender, toaster, coffee maker, air fryer"]},
        100902: {"name": "个人护理", "prompts": ["a photo of personal care device, hair dryer, shaver, electric toothbrush"]},
        100903: {"name": "清洁电器", "prompts": ["a photo of cleaning appliance, vacuum cleaner, robot vacuum, steam mop"]},
        100904: {"name": "生活电器", "prompts": ["a photo of home appliance, humidifier, fan, heater, air purifier"]},
    },
    # 手机类 L2
    1010: {
        101001: {"name": "智能手机", "prompts": ["a photo of smartphone, iPhone, Android phone, mobile phone"]},
        101002: {"name": "手机配件", "prompts": ["a photo of phone case, screen protector, charger, phone accessory"]},
        101003: {"name": "平板电脑", "prompts": ["a photo of tablet, iPad, Android tablet"]},
    },
    # 电脑类 L2
    1011: {
        101101: {"name": "笔记本电脑", "prompts": ["a photo of laptop, notebook computer, MacBook"]},
        101102: {"name": "台式电脑", "prompts": ["a photo of desktop computer, PC, all-in-one computer"]},
        101103: {"name": "电脑配件", "prompts": ["a photo of computer accessory, keyboard, mouse, monitor, webcam"]},
        101104: {"name": "存储设备", "prompts": ["a photo of storage device, USB drive, external hard drive, SSD"]},
    },
    # 食品类 L2
    1012: {
        101201: {"name": "零食", "prompts": ["a photo of snacks, chips, candy, cookies, nuts"]},
        101202: {"name": "饮品", "prompts": ["a photo of beverages, tea, coffee, juice, wine"]},
        101203: {"name": "保健品", "prompts": ["a photo of supplements, vitamins, health food, protein powder"]},
        101204: {"name": "调味品", "prompts": ["a photo of condiments, sauce, spice, seasoning, oil"]},
    },
    # 玩具类 L2
    1013: {
        101301: {"name": "积木拼图", "prompts": ["a photo of building blocks, LEGO, puzzle, jigsaw"]},
        101302: {"name": "玩偶公仔", "prompts": ["a photo of doll, action figure, stuffed toy, plush toy"]},
        101303: {"name": "遥控玩具", "prompts": ["a photo of remote control toy, RC car, drone, robot toy"]},
        101304: {"name": "益智玩具", "prompts": ["a photo of educational toy, STEM toy, board game"]},
    },
    # 运动户外类 L2
    1014: {
        101401: {"name": "健身器材", "prompts": ["a photo of fitness equipment, dumbbell, yoga mat, treadmill"]},
        101402: {"name": "户外装备", "prompts": ["a photo of outdoor gear, camping tent, sleeping bag, backpack"]},
        101403: {"name": "球类运动", "prompts": ["a photo of ball sports equipment, basketball, football, tennis racket"]},
        101404: {"name": "骑行装备", "prompts": ["a photo of cycling gear, bicycle, helmet, cycling jersey"]},
    },
    # 汽车配件类 L2
    1015: {
        101501: {"name": "车内饰品", "prompts": ["a photo of car interior accessory, seat cover, floor mat"]},
        101502: {"name": "车外配件", "prompts": ["a photo of car exterior accessory, car light, bumper, spoiler"]},
        101503: {"name": "汽车电子", "prompts": ["a photo of car electronics, dash cam, GPS navigator, car charger"]},
        101504: {"name": "汽车养护", "prompts": ["a photo of car care product, car wash, wax, polish"]},
    },
    # 办公用品类 L2
    1016: {
        101601: {"name": "文具", "prompts": ["a photo of stationery, pen, pencil, notebook, ruler"]},
        101602: {"name": "打印耗材", "prompts": ["a photo of printer supplies, ink cartridge, toner, paper"]},
        101603: {"name": "办公设备", "prompts": ["a photo of office equipment, printer, scanner, shredder"]},
    },
    # 钟表类 L2
    1017: {
        101701: {"name": "机械表", "prompts": ["a photo of mechanical watch, automatic watch, luxury timepiece"]},
        101702: {"name": "石英表", "prompts": ["a photo of quartz watch, analog watch, fashion watch"]},
        101703: {"name": "智能手表", "prompts": ["a photo of smartwatch, Apple Watch, fitness tracker"]},
        101704: {"name": "座钟挂钟", "prompts": ["a photo of wall clock, desk clock, mantel clock, alarm clock"]},
    },
    # 眼镜类 L2
    1018: {
        101801: {"name": "太阳镜", "prompts": ["a photo of sunglasses, polarized sunglasses, aviator"]},
        101802: {"name": "近视眼镜", "prompts": ["a photo of prescription glasses, optical frames, reading glasses"]},
        101803: {"name": "镜框镜片", "prompts": ["a photo of eyeglass frames, lenses, spectacle frames"]},
    },
}

# ---------------------------------------------------------------------------
# 标签体系 (ViT-L-14 零样本, 全品类通用)
# ---------------------------------------------------------------------------

TAGS: Dict[str, str] = {
    # 材质
    "真皮": "a product made of genuine leather",
    "PU皮": "a product made of synthetic PU leather",
    "棉质": "a product made of cotton fabric",
    "丝绸": "a product made of silk fabric",
    "羊毛": "a product made of wool",
    "金属": "a product made of metal steel iron aluminum",
    "塑料": "a product made of plastic polymer",
    "陶瓷": "a product made of ceramic porcelain",
    "木质": "a product made of wood timber",
    "玻璃": "a product made of glass crystal",
    # 风格
    "复古风": "a vintage retro style product",
    "简约风": "a minimalist modern simple style product",
    "商务风": "a business formal professional style product",
    "运动风": "a sporty athletic style product",
    "潮流风": "a trendy streetwear fashion-forward style product",
    "奢华风": "a luxury premium high-end style product",
    # 颜色
    "红色": "a red colored product",
    "蓝色": "a blue colored product",
    "黑色": "a black colored product",
    "白色": "a white colored product",
    "棕色": "a brown colored product",
    "绿色": "a green colored product",
    "粉色": "a pink colored product",
    "灰色": "a gray silver colored product",
    "金色": "a gold golden colored product",
    # 季节
    "春季": "a spring season product",
    "夏季": "a summer season product",
    "秋季": "an autumn fall season product",
    "冬季": "a winter season product",
    # 场景
    "日常": "a daily casual everyday product",
    "派对": "a party evening occasion product",
    "户外": "an outdoor adventure product",
    "办公": "an office work professional product",
}

TAG_NAMES: List[str] = list(TAGS.keys())

# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def is_fashion(l1_code: int) -> bool:
    """判断 L1 品类是否属于时尚领域 (路由到 FashionSigLIP)"""
    return l1_code in FASHION_L1_CODES


def get_l2_for_l1(l1_code: int) -> Dict[int, dict]:
    """获取指定 L1 下的所有 L2 子品类"""
    if is_fashion(l1_code):
        return FASHION_L2.get(l1_code, {})
    return NONFASHION_L2.get(l1_code, {})


def get_l3_for_l2(l2_code: int) -> Dict[int, dict]:
    """获取指定 L2 下的所有 L3 细分 (仅时尚类有 L3)"""
    return FASHION_L3.get(l2_code, {})


def get_all_l2_codes() -> Dict[int, str]:
    """获取所有 L2 code → name 映射"""
    result = {}
    for l2_dict in FASHION_L2.values():
        for code, info in l2_dict.items():
            result[code] = info["name"]
    for l2_dict in NONFASHION_L2.values():
        for code, info in l2_dict.items():
            result[code] = info["name"]
    return result


def get_all_l3_codes() -> Dict[int, str]:
    """获取所有 L3 code → name 映射"""
    result = {}
    for l3_dict in FASHION_L3.values():
        for code, info in l3_dict.items():
            result[code] = info["name"]
    return result
