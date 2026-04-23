import os
import re
import cv2
import json
import numpy as np
import pandas as pd
from PIL import Image, ImageEnhance, ImageFilter
from paddleocr import PaddleOCR
from sklearn.cluster import KMeans
from datetime import datetime, timedelta


OUTPUT_ROOT_DIR = os.environ.get("OUTPUT_DIR", "output_mwr_ocr")


def detect_latest_run_date(output_root_dir):
    """
    自动选择 output_mwr_ocr 下最新的日期目录，例如 2026-03-30。
    """
    if not os.path.isdir(output_root_dir):
        raise FileNotFoundError(f"输出根目录不存在: {output_root_dir}")

    date_dirs = []
    for name in os.listdir(output_root_dir):
        full_path = os.path.join(output_root_dir, name)
        if os.path.isdir(full_path) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", name):
            date_dirs.append(name)

    if not date_dirs:
        raise FileNotFoundError(f"在 {output_root_dir} 下未找到日期目录")

    return sorted(date_dirs)[-1]


RUN_DATE = detect_latest_run_date(OUTPUT_ROOT_DIR)
SCREEN_DIR = os.path.join(OUTPUT_ROOT_DIR, RUN_DATE, "screens")
OUTPUT_DIR = os.path.join(OUTPUT_ROOT_DIR, RUN_DATE, "recover_failed")
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAIN_TABLE_PATH = os.path.join(OUTPUT_ROOT_DIR, RUN_DATE, f"mwr_ocr_table_{RUN_DATE}.csv")
if not os.path.exists(MAIN_TABLE_PATH):
    raise FileNotFoundError(f"主表不存在: {MAIN_TABLE_PATH}")

MAIN_TABLE_DF = pd.read_csv(MAIN_TABLE_PATH)
MAIN_TABLE_COLUMNS = MAIN_TABLE_DF.columns.tolist()


OCR_JSON_DIR_MAIN = os.path.join(OUTPUT_ROOT_DIR, RUN_DATE, "ocr_json")
COLUMN_TEMPLATE_PATH = os.path.join(OUTPUT_ROOT_DIR, RUN_DATE, "column_template.json")


def load_ocr_json_file(json_path):
    """
    主脚本保存的 OCR JSON 是一个 list[dict]，每个元素含 box/text/score。
    这里统一读成 list，失败则返回空列表。
    """
    if not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def load_cached_column_template(template_path):
    if not os.path.exists(template_path):
        return None
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        centers = data.get("column_centers") if isinstance(data, dict) else None
        if isinstance(centers, list) and len(centers) == 7:
            centers = [float(x) for x in centers]
            print(f"[TEMPLATE] loaded cached column centers: {centers}")
            return centers
    except Exception as e:
        print(f"[WARN] failed to load cached column template: {e}")
    return None


def save_column_template(template_path, centers, used_files=None):
    if not centers:
        return
    try:
        payload = {
            "run_date": RUN_DATE,
            "column_centers": [float(x) for x in centers],
            "used_files": used_files,
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(template_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[TEMPLATE] saved column centers to: {template_path}")
    except Exception as e:
        print(f"[WARN] failed to save column template: {e}")


COLUMN_TEMPLATE = None


def extract_x_centers_from_items(items):
    xs = []
    for item in items:
        text = str(item.get("text", "")).strip()
        box = item.get("box")
        if not text or not box:
            continue
        if isinstance(box, list) and len(box) >= 4 and isinstance(box[0], list):
            x_coords = [p[0] for p in box]
            x_center = sum(x_coords) / len(x_coords)
            xs.append(float(x_center))
    return xs


def learn_column_centers_from_run(ocr_json_dir, failed_screens, n_cols=7, max_files=12):
    """
    从本次主抓取已经成功保存的 ocr_json 中，自动学习 7 列的真实 x 中心。
    跳过 failed screen 对应的 json，优先用正常 screen 学模板。
    优先读取缓存模板；若缓存不存在，再重新学习并落盘。
    """
    cached = load_cached_column_template(COLUMN_TEMPLATE_PATH)
    if cached:
        return cached

    if not os.path.isdir(ocr_json_dir):
        return None

    failed_json_names = {
        os.path.splitext(name)[0] + ".json"
        for name in failed_screens
    }

    candidate_files = sorted([
        name for name in os.listdir(ocr_json_dir)
        if name.endswith(".json") and name not in failed_json_names
    ])

    all_xs = []
    used_files = 0
    for name in candidate_files:
        json_path = os.path.join(ocr_json_dir, name)
        items = load_ocr_json_file(json_path)
        xs = extract_x_centers_from_items(items)
        if len(xs) < 40:
            continue
        all_xs.extend(xs)
        used_files += 1
        if used_files >= max_files:
            break

    if len(all_xs) < 80:
        return None

    try:
        arr = np.array(all_xs, dtype=float).reshape(-1, 1)
        model = KMeans(n_clusters=n_cols, n_init=10, random_state=42)
        model.fit(arr)
        centers = sorted(float(c[0]) for c in model.cluster_centers_)
        print(f"[TEMPLATE] learned column centers from {used_files} json files: {centers}")
        save_column_template(COLUMN_TEMPLATE_PATH, centers, used_files=used_files)
        return centers
    except Exception as e:
        print(f"[WARN] failed to learn column centers: {e}")
        return None


def assign_column_by_centers(x_center, centers):
    distances = [abs(float(x_center) - c) for c in centers]
    return int(np.argmin(distances))


def detect_failed_screens(screen_dir, run_date):
    """
    自动找出本次运行中主表未覆盖到的 screen。
    逻辑：
    1) 找到 screens 目录下所有原始 screen_XXX.png（排除 _ocrprep 和其他调试图）
    2) 读取主表 mwr_ocr_table_{run_date}.csv 中已经出现过的 screen_index
    3) 返回缺失的 screen 文件名列表
    """
    all_screen_files = sorted([
        name for name in os.listdir(screen_dir)
        if name.endswith(".png")
        and "_ocrprep" not in name
        and re.search(r"_screen_\d{3}\.png$", name)
    ])

    main_table_path = os.path.join(OUTPUT_ROOT_DIR, run_date, f"mwr_ocr_table_{run_date}.csv")
    if not os.path.exists(main_table_path):
        raise FileNotFoundError(f"主表不存在: {main_table_path}")

    table_df = pd.read_csv(main_table_path)
    covered_indices = set()
    if "screen_index" in table_df.columns:
        covered_indices = set(
            int(x) for x in table_df["screen_index"].dropna().astype(int).tolist()
        )

    failed = []
    for fname in all_screen_files:
        m = re.search(r"_screen_(\d{3})\.png$", fname)
        if not m:
            continue
        screen_idx = int(m.group(1))
        if screen_idx not in covered_indices:
            failed.append(fname)

    return failed


def extract_screen_index_from_name(screen_name: str):
    m = re.search(r"_screen_(\d{3})\.png$", str(screen_name))
    if not m:
        return ""
    return int(m.group(1))



def screen_name_to_json_name(screen_name: str):
    return os.path.splitext(screen_name)[0] + ".json"



def is_same_or_close_report_mmdd(time_text: str, run_date: str) -> bool:
    """
    优先要求与本次 RUN_DATE 同月同日；
    同时允许 ±1 天容错，避免跨午夜抓取或 OCR 轻微误差导致整行被误删。
    """
    time_text = normalize_time_text(time_text)
    m = re.search(r"^(\d{2})-(\d{2})\s+\d{2}:\d{2}$", str(time_text).strip())
    if not m:
        return False

    mmdd = f"{m.group(1)}-{m.group(2)}"
    base_date = datetime.strptime(run_date, "%Y-%m-%d")
    candidates = {
        base_date.strftime("%m-%d"),
        (base_date - timedelta(days=1)).strftime("%m-%d"),
        (base_date + timedelta(days=1)).strftime("%m-%d"),
    }
    return mmdd in candidates



def enrich_recovered_df_metadata(df: pd.DataFrame, screen_name: str, run_date: str) -> pd.DataFrame:
    """
    给 recover 结果补上主表使用的元数据列，避免 full table 后半段这些列为空。
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    screen_index = extract_screen_index_from_name(screen_name)
    crawl_time_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if "screen_index" not in df.columns:
        df["screen_index"] = screen_index
    else:
        df["screen_index"] = df["screen_index"].replace("", np.nan)
        df["screen_index"] = df["screen_index"].fillna(screen_index)

    if "report_date" not in df.columns:
        df["report_date"] = run_date
    else:
        df["report_date"] = df["report_date"].replace("", np.nan)
        df["report_date"] = df["report_date"].fillna(run_date)

    if "crawl_time_local" not in df.columns:
        df["crawl_time_local"] = crawl_time_local
    else:
        df["crawl_time_local"] = df["crawl_time_local"].replace("", np.nan)
        df["crawl_time_local"] = df["crawl_time_local"].fillna(crawl_time_local)

    df["row_order_in_screen"] = list(range(len(df)))
    return df




def is_high_confidence_row(row) -> bool:
    """
    强质量过滤：
    - 前四个文本字段至少 4 个非空；
    - 时间必须合法；
    - 库水位必须是数字；
    - 日变幅允许为空，但若非空则应像数字或变化占位符。
    """
    basin = str(row.get("流域", "")).strip()
    province = str(row.get("行政区划", "")).strip()
    river = str(row.get("河名", "")).strip()
    reservoir = str(row.get("库名", "")).strip()
    time_text = str(row.get("时间", "")).strip()
    water_level = str(row.get("库水位(米)", "")).strip()
    change_value = str(row.get("日变幅(米)", "")).strip()

    text_ok = sum(1 for x in [basin, province, river, reservoir] if x) >= 4
    time_ok = is_valid_time_text(time_text)
    water_ok = looks_like_water_level_value(water_level)
    change_ok = (not change_value) or looks_like_change_value(change_value)
    return text_ok and time_ok and water_ok and change_ok


def clean_recovered_df(df: pd.DataFrame, run_date: str) -> pd.DataFrame:
    """
    recover 合并前做最后清洗：
    1) 过滤掉时间明显不是本次 RUN_DATE 的旧行；
    2) 过滤掉流域为空的行；
    3) 过滤掉库水位不合法的行；
    4) 过滤掉低置信度的弱对齐行。
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    if "时间" in df.columns:
        df["时间"] = df["时间"].astype(str).map(normalize_time_text)
        df = df[df["时间"].map(lambda x: is_valid_time_text(x) and is_same_or_close_report_mmdd(x, run_date))]

    if "流域" in df.columns:
        df["流域"] = df["流域"].fillna("").astype(str).str.strip()
        df = df[df["流域"] != ""]

    if "库水位(米)" in df.columns:
        df["库水位(米)"] = df["库水位(米)"].fillna("").astype(str).str.strip()
        df = df[df["库水位(米)"].map(looks_like_water_level_value)]

    df = df[df.apply(is_high_confidence_row, axis=1)]
    return df.reset_index(drop=True)


FAILED_SCREENS = detect_failed_screens(SCREEN_DIR, RUN_DATE)

DEBUG_IMG_DIR = os.path.join(OUTPUT_DIR, "debug_images")
TXT_DIR = os.path.join(OUTPUT_DIR, "txt")
JSON_DIR = os.path.join(OUTPUT_DIR, "json")
CSV_DIR = os.path.join(OUTPUT_DIR, "csv")

os.makedirs(DEBUG_IMG_DIR, exist_ok=True)
os.makedirs(TXT_DIR, exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

print(f"[INFO] Auto detected RUN_DATE: {RUN_DATE}")
print(f"[INFO] Auto detected failed screens: {FAILED_SCREENS}")


def normalize_time_text(text: str) -> str:
    text = str(text).strip()
    text = text.replace("O", "0").replace("o", "0")
    text = re.sub(r"(\d{2}-\d{2})(\d{2}:\d{2})", r"\1 \2", text)
    text = re.sub(r"(\d{2})\s*[-—]\s*(\d{2})\s*(\d{2})\s*[:：]\s*(\d{2})", r"\1-\2 \3:\4", text)
    text = re.sub(r"(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})", r"\1-\2 \3:\4", text)
    text = re.sub(r"(\d{2})\s*[-—]\s*(\d{2})\s*(\d{2}:\d{2})", r"\1-\2 \3", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_valid_time_text(text: str) -> bool:
    return bool(re.search(r"\d{2}-\d{2}\s+\d{2}:\d{2}", normalize_time_text(text)))


def merge_split_time_tokens(texts):
    """
    把 OCR 拆开的时间片段尽量合并，例如：
    ["03-26", "08:00"] -> ["03-26 08:00"]
    ["03", "26", "08:00"] -> ["03-26 08:00"]
    """
    merged = []
    i = 0
    while i < len(texts):
        cur = str(texts[i]).strip()
        nxt = str(texts[i + 1]).strip() if i + 1 < len(texts) else ""
        nxt2 = str(texts[i + 2]).strip() if i + 2 < len(texts) else ""

        candidate1 = normalize_time_text(f"{cur} {nxt}") if nxt else ""
        candidate2 = normalize_time_text(f"{cur}-{nxt} {nxt2}") if nxt and nxt2 else ""

        if nxt and is_valid_time_text(candidate1):
            merged.append(candidate1)
            i += 2
            continue

        if (
            nxt and nxt2
            and re.fullmatch(r"\d{2}", cur)
            and re.fullmatch(r"\d{2}", nxt)
            and re.fullmatch(r"\d{2}:\d{2}", nxt2)
        ):
            merged.append(candidate2)
            i += 3
            continue

        merged.append(normalize_time_text(cur))
        i += 1

    return merged


# === 新增辅助函数 ===
def expand_embedded_time_tokens(texts):
    """
    把 OCR 中把“库名 + 时间”粘在一起的 token 拆开。
    例如：
    ["卷桥水库 04-01 08:00"] -> ["卷桥水库", "04-01 08:00"]
    ["王英 04-01 08:00"] -> ["王英", "04-01 08:00"]
    """
    expanded = []
    for token in texts:
        token = str(token).strip()
        if not token:
            continue
        name_part, time_part = split_embedded_time(token)
        if time_part:
            if name_part:
                expanded.append(name_part)
            expanded.append(time_part)
        else:
            expanded.append(token)
    return expanded


def looks_like_change_value(text: str) -> bool:
    """
    日变幅通常是数字或 -- / —— / 一些 OCR 变体。
    """
    text = str(text).strip()
    if not text:
        return False
    if text in {"--", "——", "—", "一", "-", "--."}:
        return True
    return looks_numeric_text(text)


def looks_like_water_level_value(text: str) -> bool:
    """
    库水位通常是数字。
    """
    return looks_numeric_text(text)


def build_row_from_time_anchor(texts, headers):
    """
    以“时间”作为锚点来对齐 7 列：
    - 时间前最多 4 列，从右往左依次是：库名、河名、行政区划、流域
    - 时间后最多 2 列，依次是：库水位(米)、日变幅(米)
    """
    aligned = [""] * 7
    time_indices = [i for i, t in enumerate(texts) if is_valid_time_text(t)]
    if not time_indices:
        return None

    time_idx = time_indices[0]
    aligned[4] = normalize_time_text(texts[time_idx])

    before = [str(x).strip() for x in texts[:time_idx] if str(x).strip()]
    after = [str(x).strip() for x in texts[time_idx + 1:] if str(x).strip()]

    # 时间前，按“从右往左”填充到前四列
    before = before[-4:]
    start_col = max(0, 4 - len(before))
    for i, t in enumerate(before):
        aligned[start_col + i] = t

    # 时间后，优先识别库水位和日变幅
    if after:
        if looks_like_water_level_value(after[0]):
            aligned[5] = after[0]
            if len(after) > 1 and looks_like_change_value(after[1]):
                aligned[6] = after[1]
        else:
            # 有些 OCR 会把时间后第一个 token 识别成无关字符，这里做一次宽松兜底
            numeric_after = [x for x in after if looks_like_water_level_value(x)]
            if numeric_after:
                aligned[5] = numeric_after[0]
                remaining = [x for x in after if x != numeric_after[0]]
                for x in remaining:
                    if looks_like_change_value(x):
                        aligned[6] = x
                        break

    row = dict(zip(headers, aligned))
    row = repair_aligned_row(row)
    return row


def looks_numeric_text(text: str) -> bool:
    text = str(text).strip()
    return bool(re.fullmatch(r"[-+]?(?:\d+\.\d+|\d+)", text))


# === 行/字段错位修复辅助 ===

KNOWN_BASINS = {
    "珠江", "长江", "黄河", "淮河", "海河", "辽河", "松花江", "黑龙江", "闽江", "内陆河",
    "太湖", "三亚河", "粤东沿海诸河", "粤西沿海诸河", "浙闽台河流", "浙东沿海诸河",
    "浙西沿海诸河", "海南"
}

KNOWN_PROVINCES = {
    "北京", "天津", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江", "上海", "江苏", "浙江", "安徽", "福建",
    "江西", "山东", "河南", "湖北", "湖南", "广东", "广西", "海南", "重庆", "四川", "贵州", "云南", "西藏",
    "陕西", "甘肃", "青海", "宁夏", "新疆", "台湾", "香港", "澳门"
}

TIME_EXTRACT_RE = re.compile(r"^(.*?)(\d{2}-\d{2}\s*\d{2}:\d{2})$")


def looks_like_basin(text: str) -> bool:
    text = str(text).strip()
    if not text:
        return False
    if text in KNOWN_BASINS:
        return True
    return any(text.startswith(x) for x in KNOWN_BASINS)


def looks_like_province(text: str) -> bool:
    return str(text).strip() in KNOWN_PROVINCES


def split_embedded_time(text: str):
    """
    把“卷桥水库 04-01 08:00”拆成（卷桥水库, 04-01 08:00）。
    """
    text = str(text).strip()
    m = TIME_EXTRACT_RE.match(text)
    if not m:
        return text, ""
    prefix = m.group(1).strip()
    tm = normalize_time_text(m.group(2))
    return prefix, tm


def repair_aligned_row(row: dict) -> dict:
    """
    修复 recover 中最常见的错位：
    1) 流域为空；
    2) 行政区划里其实是流域（如“长江”）；
    3) 河名里其实是省份（如“湖北”）；
    4) 时间列里混入“库名 + 时间”。
    """
    row = row.copy()

    basin = str(row.get("流域", "")).strip()
    province = str(row.get("行政区划", "")).strip()
    river = str(row.get("河名", "")).strip()
    reservoir = str(row.get("库名", "")).strip()
    time_text = str(row.get("时间", "")).strip()

    embedded_name, embedded_time = split_embedded_time(time_text)

    # 典型错位：空流域 + 行政区划像流域 + 河名像省份 + 时间里带库名
    if (
        not basin
        and looks_like_basin(province)
        and looks_like_province(river)
        and embedded_time
    ):
        row["流域"] = province
        row["行政区划"] = river
        row["河名"] = reservoir
        row["库名"] = embedded_name if embedded_name else reservoir
        row["时间"] = embedded_time
        return row

    # 次常见：时间里带库名，但前四列本身没有完全错位
    if embedded_time and embedded_name:
        row["时间"] = embedded_time
        if not reservoir:
            row["库名"] = embedded_name
        elif embedded_name != reservoir and embedded_name not in reservoir:
            # 若 OCR 把河名/库名少了一格，优先把带时间的前缀放到库名
            row["库名"] = embedded_name

    return row




def row_quality_score(table_rows):
    if not table_rows:
        return 0.0

    good = 0
    for row in table_rows:
        has_time = is_valid_time_text(row.get("时间", ""))
        text_cols = [row.get("流域", ""), row.get("行政区划", ""), row.get("河名", ""), row.get("库名", "")]
        non_empty_text_cols = sum(1 for x in text_cols if str(x).strip())
        numeric_cols = [row.get("库水位(米)", ""), row.get("日变幅(米)", "")]
        numeric_hits = sum(1 for x in numeric_cols if looks_numeric_text(x))

        if has_time and non_empty_text_cols >= 3 and numeric_hits >= 1:
            good += 1

    return good / max(len(table_rows), 1)


def to_json_safe(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [to_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    return value


def pil_enhance_gray(pil_img: Image.Image):
    img = pil_img.convert("L")
    img = ImageEnhance.Contrast(img).enhance(2.0)
    img = img.resize((int(img.width * 2.0), int(img.height * 2.0)), Image.Resampling.LANCZOS)
    img = img.filter(ImageFilter.SHARPEN)
    return img


def cv_versions(img_bgr):
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

    versions = {}

    # 1. 原灰度放大
    gray_big = cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    versions["gray_big"] = gray_big

    # 2. 全局二值
    _, th_bin = cv2.threshold(gray_big, 180, 255, cv2.THRESH_BINARY)
    versions["binary"] = th_bin

    # 3. 自适应阈值
    th_adapt = cv2.adaptiveThreshold(
        gray_big, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 11
    )
    versions["adaptive"] = th_adapt

    # 4. 锐化
    blur = cv2.GaussianBlur(gray_big, (0, 0), 1.0)
    sharp = cv2.addWeighted(gray_big, 1.5, blur, -0.5, 0)
    versions["sharp"] = sharp

    # 5. 锐化后二值
    _, sharp_bin = cv2.threshold(sharp, 180, 255, cv2.THRESH_BINARY)
    versions["sharp_binary"] = sharp_bin

    return versions


def save_debug_versions(screen_name, pil_img):
    paths = {}

    base = os.path.splitext(screen_name)[0]

    # PIL 版
    pil_proc = pil_enhance_gray(pil_img)
    pil_path = os.path.join(DEBUG_IMG_DIR, f"{base}_pil_enhance.png")
    pil_proc.save(pil_path)
    paths["pil_enhance"] = pil_path

    # OpenCV 版
    img_bgr = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    versions = cv_versions(img_bgr)

    for name, arr in versions.items():
        out_path = os.path.join(DEBUG_IMG_DIR, f"{base}_{name}.png")
        cv2.imwrite(out_path, arr)
        paths[name] = out_path

    return paths


def do_ocr_file(ocr_engine, image_path):
    result = ocr_engine.predict(image_path)

    parsed = []
    plain_texts = []

    if not result:
        return parsed, plain_texts

    for item in result:
        rec_texts = item.get("rec_texts", []) or []
        rec_scores = item.get("rec_scores", []) or []
        rec_polys = item.get("rec_polys", []) or []

        for idx, text in enumerate(rec_texts):
            score = float(rec_scores[idx]) if idx < len(rec_scores) else None
            box = rec_polys[idx] if idx < len(rec_polys) else None
            parsed.append({
                "box": to_json_safe(box),
                "text": str(text),
                "score": to_json_safe(score)
            })
            plain_texts.append(str(text))

    return parsed, plain_texts



def group_ocr_lines(parsed, y_threshold=14):
    rows = []
    for item in parsed:
        box = item.get("box")
        text = str(item.get("text", "")).strip()
        if not box or not text:
            continue

        xs = [pt[0] for pt in box]
        ys = [pt[1] for pt in box]
        row = {
            "text": text,
            "x_center": sum(xs) / len(xs),
            "y_center": sum(ys) / len(ys),
        }

        placed = False
        for group in rows:
            if abs(group["y_center"] - row["y_center"]) <= y_threshold:
                group["items"].append(row)
                ys2 = [it["y_center"] for it in group["items"]]
                group["y_center"] = sum(ys2) / len(ys2)
                placed = True
                break

        if not placed:
            rows.append({"y_center": row["y_center"], "items": [row]})

    rows.sort(key=lambda g: g["y_center"])
    final_rows = []
    for group in rows:
        items = sorted(group["items"], key=lambda x: x["x_center"])
        final_rows.append(items)
    return final_rows


# === 新列中心辅助函数 ===

def build_template_aligned_row(items, headers, column_centers):
    """
    用学习到的列中心，把当前行的 OCR token 映射到 7 列。
    同一列若出现多个 token，则按从左到右顺序拼接。
    """
    if not column_centers or len(column_centers) != 7:
        return None

    buckets = {i: [] for i in range(7)}
    for it in items:
        text = str(it.get("text", "")).strip()
        if not text:
            continue
        col_idx = assign_column_by_centers(it["x_center"], column_centers)
        buckets[col_idx].append((it["x_center"], text))

    aligned = []
    for i in range(7):
        parts = [t for _, t in sorted(buckets[i], key=lambda x: x[0])]
        cell_text = " ".join(parts).strip()
        aligned.append(cell_text)

    aligned[4] = normalize_time_text(aligned[4])
    row = dict(zip(headers, aligned))
    row = repair_aligned_row(row)
    return row


def choose_row_from_items(items, headers, column_centers=None):
    """
    行解析优先级：
    1) 先用时间锚点法；
    2) 时间锚点失败时，再用列模板法。
    """
    texts = [str(it.get("text", "")).strip() for it in items if str(it.get("text", "")).strip()]
    texts = merge_split_time_tokens(texts)
    texts = expand_embedded_time_tokens(texts)
    texts = [str(x).strip() for x in texts if str(x).strip()]

    if len(texts) < 3:
        return None

    joined = " ".join(texts)
    if "报表日期" in joined or "库水位(米)" in joined:
        return None

    row = build_row_from_time_anchor(texts, headers)
    if row is not None:
        return row

    if column_centers:
        return build_template_aligned_row(items, headers, column_centers)

    return None


def rows_to_table(grouped_rows, column_centers=None):
    headers = ["流域", "行政区划", "河名", "库名", "时间", "库水位(米)", "日变幅(米)"]
    table = []

    for items in grouped_rows:
        row = choose_row_from_items(items, headers, column_centers=column_centers)
        if row is None:
            continue

        if not is_valid_time_text(row.get("时间", "")):
            continue
        if not looks_like_water_level_value(row.get("库水位(米)", "")):
            continue

        table.append(row)

    return table


# === 新增辅助函数 ===
def get_main_data_columns(main_df: pd.DataFrame):
    """
    用主表作为最终字段模板。优先保留主表已有列顺序，保证 recover 只是在主表基础上补充。
    """
    preferred = [
        "流域", "行政区划", "河名", "库名", "时间", "库水位(米)", "日变幅(米)",
        "screen_index", "crawl_time_local", "report_date", "row_order_in_screen"
    ]
    cols = [c for c in preferred if c in main_df.columns]
    remaining = [c for c in main_df.columns if c not in cols]
    return cols + remaining



def normalize_recovered_df_to_main_schema(recovered_df: pd.DataFrame, main_df: pd.DataFrame) -> pd.DataFrame:
    """
    按主表字段模板整理 recover 结果：
    1) 缺失列补空
    2) 多余列丢弃
    3) 列顺序与主表保持一致
    """
    target_cols = get_main_data_columns(main_df)

    if recovered_df is None or recovered_df.empty:
        return pd.DataFrame(columns=target_cols)

    recovered_df = recovered_df.copy()
    for col in target_cols:
        if col not in recovered_df.columns:
            recovered_df[col] = ""

    recovered_df = recovered_df[target_cols]
    return recovered_df



def get_merge_dedup_columns(final_df: pd.DataFrame):
    """
    最终去重严格以主表真实业务字段为准，不依赖 recover 过程中的额外调试列。
    """
    preferred = ["report_date", "流域", "行政区划", "河名", "库名", "时间", "库水位(米)", "日变幅(米)"]
    dedup_cols = [c for c in preferred if c in final_df.columns]
    if dedup_cols:
        return dedup_cols

    fallback = ["流域", "行政区划", "河名", "库名", "时间", "库水位(米)", "日变幅(米)"]
    return [c for c in fallback if c in final_df.columns]


def choose_best_ocr(ocr_engine, screen_name):
    img_path = os.path.join(SCREEN_DIR, screen_name)
    pil_img = Image.open(img_path).convert("RGB")
    version_paths = save_debug_versions(screen_name, pil_img)

    global COLUMN_TEMPLATE
    if COLUMN_TEMPLATE is None:
        COLUMN_TEMPLATE = learn_column_centers_from_run(
            OCR_JSON_DIR_MAIN,
            FAILED_SCREENS,
            n_cols=7,
            max_files=12,
        )

    candidates = []
    for version_name, version_path in version_paths.items():
        parsed, plain_texts = do_ocr_file(ocr_engine, version_path)
        grouped_rows = group_ocr_lines(parsed)
        table_rows = rows_to_table(grouped_rows, column_centers=COLUMN_TEMPLATE)
        quality = row_quality_score(table_rows)

        candidates.append({
            "version_name": version_name,
            "version_path": version_path,
            "parsed": parsed,
            "plain_texts": plain_texts,
            "table_rows": table_rows,
            "quality": quality,
        })

        print(
            f"[INFO] {screen_name} | version={version_name} | "
            f"ocr_lines={len(plain_texts)} | table_rows={len(table_rows)} | quality={quality:.3f}"
        )

    best = max(
        candidates,
        key=lambda x: (
            x["quality"],
            len(x["table_rows"]),
            sum(1 for r in x["table_rows"] if is_valid_time_text(r.get("时间", ""))),
            len(x["plain_texts"]),
        ),
    )
    return best


def merge_main_and_recovered(run_date, output_root_dir, recovered_rows):
    """
    自动合并主表 + recover_failed，生成最终全量表。
    以主表 mwr_ocr_table_{date}.csv 为准作为“干净初稿”，recover 结果只做补充。
    """
    main_table_path = os.path.join(output_root_dir, run_date, f"mwr_ocr_table_{run_date}.csv")
    if not os.path.exists(main_table_path):
        raise FileNotFoundError(f"主表不存在: {main_table_path}")

    main_df = pd.read_csv(main_table_path)
    recovered_df = pd.DataFrame(recovered_rows)
    recovered_df = normalize_recovered_df_to_main_schema(recovered_df, main_df)
    recovered_df = clean_recovered_df(recovered_df, run_date)
    main_df = normalize_recovered_df_to_main_schema(main_df, main_df)

    if recovered_df.empty:
        final_df = main_df.copy()
    else:
        final_df = pd.concat([main_df, recovered_df], ignore_index=True)

    dedup_cols = get_merge_dedup_columns(final_df)
    if dedup_cols:
        final_df = final_df.drop_duplicates(subset=dedup_cols, keep="first")
    else:
        final_df = final_df.drop_duplicates(keep="first")

    target_cols = get_main_data_columns(main_df)
    final_df = final_df[target_cols]

    final_path = os.path.join(output_root_dir, run_date, f"mwr_ocr_full_table_{run_date}.csv")
    final_df.to_csv(final_path, index=False, encoding="utf-8-sig")
    return final_path, len(main_df), len(recovered_df), len(final_df)


def main():
    ocr_engine = PaddleOCR(use_textline_orientation=False, lang="ch")

    global COLUMN_TEMPLATE
    COLUMN_TEMPLATE = learn_column_centers_from_run(
        OCR_JSON_DIR_MAIN,
        FAILED_SCREENS,
        n_cols=7,
        max_files=12,
    )

    all_rows = []
    if not FAILED_SCREENS:
        print("[INFO] No failed screens detected. Will directly generate full table from main table.")
    diagnostics = []

    for screen_name in FAILED_SCREENS:
        print(f"\n[INFO] Recovering {screen_name}")
        best = choose_best_ocr(ocr_engine, screen_name)
        diagnostics.append({
            "screen_name": screen_name,
            "selected_version": best["version_name"],
            "ocr_lines": len(best["plain_texts"]),
            "structured_rows": len(best["table_rows"]),
            "quality": round(best["quality"], 3),
            "first_row": json.dumps(best["table_rows"][0], ensure_ascii=False) if best["table_rows"] else "",
            "last_row": json.dumps(best["table_rows"][-1], ensure_ascii=False) if best["table_rows"] else "",
        })

        base = os.path.splitext(screen_name)[0]

        txt_path = os.path.join(TXT_DIR, f"{base}_best.txt")
        json_path = os.path.join(JSON_DIR, f"{base}_best.json")
        csv_path = os.path.join(CSV_DIR, f"{base}_best.csv")

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(best["plain_texts"]))

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(to_json_safe(best["parsed"]), f, ensure_ascii=False, indent=2)

        if best["table_rows"]:
            df = pd.DataFrame(best["table_rows"])
            df = enrich_recovered_df_metadata(df, screen_name, RUN_DATE)
            df = clean_recovered_df(df, RUN_DATE)
            df = normalize_recovered_df_to_main_schema(df, MAIN_TABLE_DF)
            df = df.drop_duplicates(keep="first")
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            all_rows.extend(df.to_dict(orient="records"))

        print(
            f"[SAVED] best version for {screen_name}: {best['version_name']} | "
            f"ocr_lines={len(best['plain_texts'])} | rows={len(best['table_rows'])} | quality={best['quality']:.3f}"
        )

    final_full_path, main_count, recovered_count, final_count = merge_main_and_recovered(
        RUN_DATE,
        OUTPUT_ROOT_DIR,
        all_rows,
    )
    print(
        f"[SAVED] {final_full_path} | main_rows={main_count} | "
        f"recovered_rows={recovered_count} | final_rows={final_count}"
    )

    if diagnostics:
        diag_path = os.path.join(OUTPUT_DIR, f"recovered_failed_screens_diagnostics_{RUN_DATE}.csv")
        pd.DataFrame(diagnostics).to_csv(diag_path, index=False, encoding="utf-8-sig")
        print(f"[SAVED] {diag_path}")

    if all_rows:
        final_path = os.path.join(OUTPUT_DIR, f"recovered_failed_screens_merged_{RUN_DATE}.csv")
        recover_df = pd.DataFrame(all_rows)
        recover_df = clean_recovered_df(recover_df, RUN_DATE)
        recover_df = normalize_recovered_df_to_main_schema(recover_df, MAIN_TABLE_DF)
        recover_df.to_csv(final_path, index=False, encoding="utf-8-sig")
        print(f"[SAVED] {final_path}")


if __name__ == "__main__":
    main()