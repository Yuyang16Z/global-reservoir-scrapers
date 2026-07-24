import os
import time
import json
import io
from PIL import Image, ImageEnhance, ImageFilter
from datetime import datetime
import re
import statistics

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from paddleocr import PaddleOCR
import numpy as np
os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")


URL = "http://xxfb.mwr.cn/sq_dxsk.html?v=1.0"



OUTPUT_ROOT_DIR = os.environ.get("OUTPUT_DIR", "output_mwr_ocr")
# 是否对截图做预处理后再 OCR。默认关闭以提升速度；需要更高识别率时可设为 True。
USE_OCR_PREPROCESS = False
# 是否以 headless 模式启动 Chrome。服务器（Oracle / Actions）必须 headless；
# 本地调试时可以设 MWR_HEADLESS=0 看到浏览器窗口。
HEADLESS = os.environ.get("MWR_HEADLESS", "1") != "0"


def build_output_paths(report_date):
    """
    按报表日期创建当天独立输出目录，方便每天运行一次分别存档。
    例如：output_mwr_ocr/2026-03-27/
    """
    output_dir = os.path.join(OUTPUT_ROOT_DIR, report_date)
    screen_dir = os.path.join(output_dir, "screens")
    ocr_json_dir = os.path.join(output_dir, "ocr_json")
    ocr_txt_dir = os.path.join(output_dir, "ocr_txt")

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(screen_dir, exist_ok=True)
    os.makedirs(ocr_json_dir, exist_ok=True)
    os.makedirs(ocr_txt_dir, exist_ok=True)

    return {
        "output_dir": output_dir,
        "screen_dir": screen_dir,
        "ocr_json_dir": ocr_json_dir,
        "ocr_txt_dir": ocr_txt_dir,
    }


def setup_driver(headless=False):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1800,2200")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--force-device-scale-factor=1")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    try:
        driver.maximize_window()
    except Exception:
        pass
    return driver


def wait_page_loaded(driver, seconds=6):
    time.sleep(seconds)


# ---------------------- 新增辅助函数 ----------------------

def get_report_date_from_page(driver):
    """
    优先从页面顶部的“报表日期：YYYY年MM月DD日”提取报表日期。
    如果取不到，再尝试从首行时间列中提取 MM-DD，并补上当前年份。
    最后才回退到本地日期。
    """
    # 先找页面顶部完整文本
    text = driver.execute_script(
        """
        return document.body ? (document.body.innerText || document.body.textContent || '') : '';
        """
    ) or ""

    m = re.search(r"报表日期[:：]\s*(\d{4})年(\d{2})月(\d{2})日", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 再尝试从表格第一行时间列取 MM-DD HH:MM
    values = driver.execute_script(
        """
        const row = document.querySelector('#DataContainer tr');
        if (!row) return [];
        return Array.from(row.querySelectorAll('td')).map(td => (td.innerText || td.textContent || '').trim());
        """
    ) or []

    for v in values:
        m2 = re.search(r"(\d{2})-(\d{2})\s+\d{2}:\d{2}", v)
        if m2:
            year = datetime.now().year
            return f"{year}-{m2.group(1)}-{m2.group(2)}"

    return datetime.now().strftime("%Y-%m-%d")
# ---------------------- 新增辅助函数 ----------------------

def wait_until_table_ready(driver, timeout=25):
    """
    等待表格首批数据、字体替换和时间列都真正加载完成。
    """
    start = time.time()
    last_reason = "unknown"
    while time.time() - start < timeout:
        try:
            state = driver.execute_script(
                """
                const rows = document.querySelectorAll('#DataContainer tr');
                if (!rows.length) {
                    return {ready: false, reason: 'no_rows'};
                }
                const first = rows[0];
                const tds = first.querySelectorAll('td');
                if (tds.length < 7) {
                    return {ready: false, reason: 'not_enough_cells'};
                }
                const values = Array.from(tds).map(td => (td.innerText || td.textContent || '').trim());
                const allEmpty = values.every(v => !v);
                if (allEmpty) {
                    return {ready: false, reason: 'all_empty'};
                }
                const hasTime = values.some(v => /\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}/.test(v));
                const nonEmptyCount = values.filter(Boolean).length;
                const hasFont = Array.from(tds).some(td => {
                    const ff = getComputedStyle(td).fontFamily || '';
                    return ff.includes('cfg_');
                });
                if (!hasTime) {
                    return {ready: false, reason: 'time_not_ready', values};
                }
                if (nonEmptyCount < 5) {
                    return {ready: false, reason: 'too_few_non_empty', values};
                }
                return {ready: true, reason: 'ok', values, hasFont};
                """
            )
            if state.get("ready"):
                time.sleep(1.0)
                return state
            last_reason = state.get("reason", "unknown")
        except Exception as e:
            last_reason = str(e)
        time.sleep(0.5)
    raise TimeoutError(f"表格数据在规定时间内没有准备好: {last_reason}")


# ---------------------- 新增字体检测辅助函数 ----------------------
def wait_until_visible_fonts_ready(driver, container, timeout=15):
    """
    等待当前可视区域中的字体真正加载并稳定，避免前几屏截图还是伪字/生僻字。
    """
    start = time.time()
    last_state = None

    while time.time() - start < timeout:
        try:
            state = driver.execute_script(
                """
                const container = arguments[0];
                const crect = container.getBoundingClientRect();
                const visibleTds = Array.from(document.querySelectorAll('#DataContainer td')).filter(td => {
                    const r = td.getBoundingClientRect();
                    return r.bottom > crect.top && r.top < crect.bottom;
                });

                const visibleTexts = visibleTds
                    .map(td => (td.innerText || td.textContent || '').trim())
                    .filter(Boolean);

                const fontFamilies = [...new Set(
                    visibleTds
                        .map(td => getComputedStyle(td).fontFamily || '')
                        .filter(ff => ff.includes('cfg_'))
                )];

                const fontsReady = fontFamilies.every(ff => {
                    try {
                        return document.fonts.check('16px ' + ff);
                    } catch (e) {
                        return false;
                    }
                });

                return {
                    visibleCount: visibleTds.length,
                    textCount: visibleTexts.length,
                    fontFamilies,
                    fontsReady,
                    fontsStatus: document.fonts ? document.fonts.status : 'unknown'
                };
                """,
                container,
            )
            last_state = state
            if state["visibleCount"] > 0 and state["textCount"] > 20 and state["fontsReady"]:
                time.sleep(0.6)
                return state
        except Exception as e:
            last_state = {"error": str(e)}
        time.sleep(0.5)

    raise TimeoutError(f"可视区域字体未稳定: {last_state}")


def warm_up_visible_region(driver, container):
    """
    轻微抖动一次可视区域，触发页面的字体替换/可视区监听逻辑。
    """
    current_top = driver.execute_script("return arguments[0].scrollTop;", container)
    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 2;", container)
    time.sleep(0.25)
    driver.execute_script("arguments[0].scrollTop = arguments[1];", container, current_top)
    time.sleep(0.4)


def scroll_page_to_container_top(driver, elem, top_margin=20):
    """
    让整个网页滚动，把表格容器尽量放到视口靠上位置，方便截取更多可视区域。
    """
    driver.execute_script(
        """
        const el = arguments[0];
        const topMargin = arguments[1];
        const rect = el.getBoundingClientRect();
        const absoluteTop = window.scrollY + rect.top;
        window.scrollTo({top: Math.max(0, absoluteTop - topMargin), behavior: 'instant'});
        """,
        elem,
        top_margin,
    )


def expand_container_height(driver, elem, bottom_margin=20, min_height=720):
    """
    尝试把内部滚动容器拉高，尽量让每次截图包含更多行。
    """
    driver.execute_script(
        """
        const el = arguments[0];
        const bottomMargin = arguments[1];
        const minHeight = arguments[2];
        const rect = el.getBoundingClientRect();
        const available = Math.max(minHeight, Math.floor(window.innerHeight - rect.top - bottomMargin));
        el.style.height = available + 'px';
        el.style.maxHeight = available + 'px';
        el.style.overflowY = 'scroll';
        """,
        elem,
        bottom_margin,
        min_height,
    )


def get_row_scroll_positions(driver, container):
    """
    读取每一行在内部滚动容器坐标系里的顶部位置，用于精确滚动到整行边界。
    """
    return driver.execute_script(
        """
        const container = arguments[0];
        const rows = Array.from(document.querySelectorAll('#DataContainer tr'));
        const crect = container.getBoundingClientRect();
        return rows.map((tr, idx) => {
            const r = tr.getBoundingClientRect();
            return {
                index: idx,
                top: Math.round(container.scrollTop + (r.top - crect.top)),
                height: Math.round(r.height)
            };
        }).filter(x => x.height > 0);
        """,
        container,
    )


def scroll_to_next_aligned_page(driver, container, overlap_rows=1):
    """
    不按固定像素滚动，而是滚动到下一屏第一个完整行的顶部，避免出现半行。
    返回新 scrollTop；如果到底部无法继续则返回当前 scrollTop。
    """
    info = get_container_info(driver, container)
    current_top = int(info["scrollTop"])
    client_height = int(info["clientHeight"])
    rows = get_row_scroll_positions(driver, container)
    if not rows:
        return current_top

    fully_visible = [r for r in rows if r["top"] >= current_top and (r["top"] + r["height"]) <= (current_top + client_height - 4)]
    if fully_visible:
        target_idx = fully_visible[-1]["index"] - overlap_rows + 1
    else:
        next_rows = [r for r in rows if r["top"] > current_top + 20]
        target_idx = next_rows[0]["index"] if next_rows else rows[-1]["index"]

    target_idx = max(0, min(target_idx, len(rows) - 1))
    target_top = rows[target_idx]["top"]
    max_scroll = max(0, int(info["scrollHeight"]) - client_height)
    target_top = max(0, min(target_top, max_scroll))

    driver.execute_script("arguments[0].scrollTop = arguments[1];", container, target_top)
    time.sleep(0.8)
    return int(get_container_info(driver, container)["scrollTop"])


def find_scroll_container(driver):
    """
    优先使用真正可滚动的外层容器，而不是 tbody。
    从你给的结构看，真正的滚动容器是 #hdtable.ssdiv。
    """
    candidates = [
        "#hdtable.ssdiv",
        "#hdtable",
        "div#hdtable",
        "div.ssdiv",
        "#hdcontent",
    ]

    for css in candidates:
        elems = driver.find_elements(By.CSS_SELECTOR, css)
        if not elems:
            continue
        for elem in elems:
            info = driver.execute_script(
                """
                const el = arguments[0];
                return {
                    scrollHeight: el.scrollHeight || 0,
                    clientHeight: el.clientHeight || 0,
                    overflowY: getComputedStyle(el).overflowY || ''
                };
                """,
                elem,
            )
            if info["scrollHeight"] > info["clientHeight"] or info["overflowY"] in ["scroll", "auto"]:
                return elem, css

    raise RuntimeError("没有找到真正可滚动的表格容器，请重新检查页面结构。")


def get_container_info(driver, elem):
    info = driver.execute_script("""
        const el = arguments[0];
        const rect = el.getBoundingClientRect();
        return {
            x: Math.round(rect.left),
            y: Math.round(rect.top),
            width: Math.round(rect.width),
            height: Math.round(rect.height),
            scrollTop: el.scrollTop,
            scrollHeight: el.scrollHeight,
            clientHeight: el.clientHeight
        };
    """, elem)
    return info

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


# ---------------------- OCR 预处理和辅助函数 ----------------------
def preprocess_image_for_ocr(image_path):
    """
    对截图做轻量增强，提升 OCR 对中文和数字的识别率。
    当 USE_OCR_PREPROCESS=False 时，直接返回原图以加快速度。
    """
    if not USE_OCR_PREPROCESS:
        return image_path

    img = Image.open(image_path).convert("L")
    img = ImageEnhance.Contrast(img).enhance(1.8)
    img = img.resize((int(img.width * 1.8), int(img.height * 1.8)), Image.Resampling.LANCZOS)
    img = img.filter(ImageFilter.SHARPEN)

    out_path = image_path.replace(".png", "_ocrprep.png")
    img.save(out_path)
    return out_path


def normalize_time_text(text):
    text = str(text).strip()
    text = re.sub(r"(\d{2}-\d{2})(\d{2}:\d{2})", r"\1 \2", text)
    return text


def is_valid_time_text(text):
    return bool(re.search(r"\d{2}-\d{2}\s+\d{2}:\d{2}", normalize_time_text(text)))


def looks_numeric_text(text):
    text = str(text).strip()
    return bool(re.fullmatch(r"[-+]?(?:\d+\.\d+|\d+)", text))


def row_quality_score(table_rows):
    """
    统计一屏里“像真实水库行”的比例。
    真实行通常应当至少有：时间列 + 第1~4列里若干中文字段 + 第6/7列至少一个数字。
    """
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


def save_element_screenshot(driver, elem, save_path):
    save_container_screenshot_with_left_padding(
        driver,
        elem,
        save_path,
        left_padding=52,
        top_padding=2,
        right_padding=8,
        bottom_padding=2,
    )


def save_container_screenshot_with_left_padding(driver, elem, save_path, left_padding=40, top_padding=0, right_padding=0, bottom_padding=0):
    """
    截取真正的滚动容器，在左边额外留一点边，并给上下各留少量安全边，尽量保住首列和边界行。
    """
    png = driver.get_screenshot_as_png()
    full_img = Image.open(io.BytesIO(png))

    rect = driver.execute_script(
        """
        const el = arguments[0];
        const r = el.getBoundingClientRect();
        return {
            left: r.left,
            top: r.top,
            right: r.right,
            bottom: r.bottom,
            dpr: window.devicePixelRatio || 1
        };
        """,
        elem,
    )

    dpr = rect["dpr"]
    left = max(0, int((rect["left"] - left_padding) * dpr))
    top = max(0, int((rect["top"] - top_padding) * dpr))
    right = min(full_img.width, int((rect["right"] + right_padding) * dpr))
    bottom = min(full_img.height, int((rect["bottom"] + bottom_padding) * dpr))

    cropped = full_img.crop((left, top, right, bottom))
    cropped.save(save_path)


def do_ocr(ocr_engine, image_path):
    ocr_input_path = preprocess_image_for_ocr(image_path)
    if ocr_input_path == image_path:
        result = ocr_engine.predict(image_path)
    else:
        result = ocr_engine.predict(ocr_input_path)

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

def capture_and_parse_screen(driver, container, img_path, ocr_engine, report_date, crawl_time_local, screen_index, min_rows_retry_threshold=18):
    """
    截图 + OCR + 组表。
    如果某一屏解析出的表格行数明显偏少，或整体行质量偏低，则自动重截一次并取更好的结果。
    """
    attempts = []

    for attempt in range(2):
        warm_up_visible_region(driver, container)
        visible_font_state = wait_until_visible_fonts_ready(driver, container, timeout=15)
        print(f"[INFO] Visible fonts ready (attempt {attempt + 1}): {visible_font_state}")

        save_element_screenshot(driver, container, img_path)
        print(f"[SAVED] {img_path} (attempt {attempt + 1})")

        parsed, plain_texts = do_ocr(ocr_engine, img_path)
        grouped_rows = group_ocr_lines(parsed)
        table_rows = rows_to_table(grouped_rows)
        quality = row_quality_score(table_rows)

        for row in table_rows:
            row["report_date"] = report_date
            row["crawl_time_local"] = crawl_time_local
            row["screen_index"] = screen_index

        attempts.append({
            "attempt": attempt + 1,
            "parsed": parsed,
            "plain_texts": plain_texts,
            "table_rows": table_rows,
            "quality": quality,
            "visible_font_state": visible_font_state,
        })

        print(f"[INFO] OCR lines (attempt {attempt + 1}): {len(plain_texts)}")
        print(f"[INFO] Parsed table rows (attempt {attempt + 1}): {len(table_rows)}")
        print(f"[INFO] Row quality score (attempt {attempt + 1}): {quality:.3f}")
        if table_rows:
            print(f"[INFO] First parsed row (attempt {attempt + 1}): {table_rows[0]}")
            print(f"[INFO] Last parsed row (attempt {attempt + 1}): {table_rows[-1]}")

        if len(table_rows) >= min_rows_retry_threshold and quality >= 0.75:
            break

        if attempt == 0:
            print(f"[WARN] Screen quality not good enough (rows={len(table_rows)}, threshold={min_rows_retry_threshold}, quality={quality:.3f}), retrying this screen...")
            time.sleep(0.8)

    best = max(attempts, key=lambda x: (x["quality"], len(x["table_rows"]), len(x["plain_texts"])))
    if len(attempts) > 1:
        print(
            f"[INFO] Selected attempt {best['attempt']} for screen {screen_index} "
            f"with quality={best['quality']:.3f}, {len(best['table_rows'])} parsed rows and {len(best['plain_texts'])} OCR lines"
        )

    return best["parsed"], best["plain_texts"], best["table_rows"]


def scroll_one_page(driver, elem):
    return scroll_to_next_aligned_page(driver, elem, overlap_rows=1)


def scroll_to_top(driver, elem):
    driver.execute_script("arguments[0].scrollTop = 0;", elem)


def group_ocr_lines(parsed, y_threshold=18):
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
            "x_min": min(xs),
            "x_max": max(xs),
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


def rows_to_table(grouped_rows):
    headers = ["流域", "行政区划", "河名", "库名", "时间", "库水位(米)", "日变幅(米)"]
    table = []

    for items in grouped_rows:
        row_order_in_screen = len(table)
        texts = [str(it["text"]).strip() for it in items if str(it.get("text", "")).strip()]

        if len(texts) < 3:
            continue
        if "报表日期" in " ".join(texts):
            continue
        if texts[:3] == headers[:3] or "库水位(米)" in texts:
            continue

        aligned = [""] * 7
        time_indices = [i for i, t in enumerate(texts) if is_valid_time_text(t)]

        if time_indices:
            time_idx = time_indices[0]
            aligned[4] = normalize_time_text(texts[time_idx])

            before = texts[:time_idx][-4:]
            start_col = max(0, 4 - len(before))
            for i, t in enumerate(before):
                aligned[start_col + i] = t

            after = texts[time_idx + 1:]
            if after:
                aligned[5] = after[0]
            if len(after) > 1:
                aligned[6] = after[1]
        else:
            if len(texts) > 7:
                texts = texts[:7]
            elif len(texts) < 7:
                texts = texts + [""] * (7 - len(texts))
            aligned = texts
            aligned[4] = normalize_time_text(aligned[4])

        row = dict(zip(headers, aligned))
        row["row_order_in_screen"] = row_order_in_screen

        if not is_valid_time_text(row.get("时间", "")):
            continue

        table.append(row)
    return table
def get_dynamic_retry_threshold(recent_row_counts, default_threshold=18):
    """
    根据最近若干屏的正常解析行数，动态决定当前屏是否需要重试。
    比固定 <18 更智能：当近期正常值较高时自动提高标准；当近期偏低时适当放宽。
    """
    valid_counts = [x for x in recent_row_counts if isinstance(x, int) and x > 0]
    if not valid_counts:
        return default_threshold

    sample = valid_counts[-6:]
    baseline = statistics.median(sample)
    threshold = int(round(baseline * 0.75))
    threshold = max(14, threshold)
    threshold = min(24, threshold)
    return threshold


def main():
    local_today = datetime.now().strftime("%Y-%m-%d")
    crawl_time_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    driver = setup_driver(headless=HEADLESS)
    ocr_engine = PaddleOCR(use_textline_orientation=False, lang="ch")
    print(f"[INFO] USE_OCR_PREPROCESS={USE_OCR_PREPROCESS}")

    try:
        print(f"[INFO] Opening {URL}")
        driver.get(URL)
        print("[INFO] Waiting for page to load...")
        wait_page_loaded(driver, seconds=8)
        print("[INFO] Page load wait complete.")
        ready_state = wait_until_table_ready(driver, timeout=30)
        print(f"[INFO] Table ready: {ready_state}")
        time.sleep(1.0)
        report_date = get_report_date_from_page(driver)
        print(f"[INFO] Report date from page: {report_date}")
        output_paths = build_output_paths(report_date)
        OUTPUT_DIR = output_paths["output_dir"]
        SCREEN_DIR = output_paths["screen_dir"]
        OCR_JSON_DIR = output_paths["ocr_json_dir"]
        OCR_TXT_DIR = output_paths["ocr_txt_dir"]
        print(f"[INFO] Output directory for this run: {OUTPUT_DIR}")

        container, css_used = find_scroll_container(driver)
        driver.execute_script("arguments[0].scrollLeft = 0;", container)
        scroll_page_to_container_top(driver, container, top_margin=12)
        time.sleep(0.5)
        expand_container_height(driver, container, bottom_margin=18, min_height=820)
        time.sleep(0.5)
        print(f"[INFO] Found container: {css_used}")
        print(f"[INFO] Container info: {get_container_info(driver, container)}")
        print(f"[INFO] Row positions sample: {get_row_scroll_positions(driver, container)[:5]}")

        scroll_to_top(driver, container)
        driver.execute_script("arguments[0].scrollLeft = 0;", container)
        time.sleep(1.2)
        warm_up_visible_region(driver, container)
        visible_font_state = wait_until_visible_fonts_ready(driver, container, timeout=20)
        print(f"[INFO] Initial visible fonts ready: {visible_font_state}")

        seen_positions = set()
        records = []
        extracted_rows = []
        recent_row_counts = []

        max_loops = 300
        last_scroll_top = -1

        for idx in range(max_loops):
            info = get_container_info(driver, container)

            scroll_top = int(info["scrollTop"])
            scroll_height = int(info["scrollHeight"])
            client_height = int(info["clientHeight"])

            print(f"[INFO] Screen {idx:03d} | scrollTop={scroll_top} | clientHeight={client_height} | scrollHeight={scroll_height}")
            visible_rows = driver.execute_script(
                """
                const container = arguments[0];
                const crect = container.getBoundingClientRect();
                const rows = Array.from(document.querySelectorAll('#DataContainer tr'));
                return rows.map((tr, idx) => {
                    const r = tr.getBoundingClientRect();
                    return {idx, top: r.top, bottom: r.bottom};
                }).filter(x => x.bottom > crect.top && x.top < crect.bottom).slice(0, 3);
                """,
                container,
            )
            print(f"[INFO] Visible rows sample: {visible_rows}")

            # 防止重复截图
            if scroll_top in seen_positions:
                print("[INFO] Detected repeated scroll position, stopping.")
                break
            seen_positions.add(scroll_top)

            img_name = f"{report_date}_{timestamp}_screen_{idx:03d}.png"
            img_path = os.path.join(SCREEN_DIR, img_name)

            dynamic_retry_threshold = get_dynamic_retry_threshold(recent_row_counts, default_threshold=18)
            print(f"[INFO] Dynamic retry threshold for screen {idx}: {dynamic_retry_threshold} based on recent counts: {recent_row_counts[-6:]}")

            parsed, plain_texts, table_rows = capture_and_parse_screen(
                driver=driver,
                container=container,
                img_path=img_path,
                ocr_engine=ocr_engine,
                report_date=report_date,
                crawl_time_local=crawl_time_local,
                screen_index=idx,
                min_rows_retry_threshold=dynamic_retry_threshold,
            )
            extracted_rows.extend(table_rows)
            print(f"[INFO] Final chosen rows for screen {idx}: {len(table_rows)}")
            if len(table_rows) > 0:
                recent_row_counts.append(len(table_rows))

            json_path = os.path.join(OCR_JSON_DIR, img_name.replace(".png", ".json"))
            txt_path = os.path.join(OCR_TXT_DIR, img_name.replace(".png", ".txt"))

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(to_json_safe(parsed), f, ensure_ascii=False, indent=2)

            with open(txt_path, "w", encoding="utf-8") as f:
                f.write("\n".join(plain_texts))

            records.append({
                "report_date": report_date,
                "local_today": local_today,
                "crawl_time_local": crawl_time_local,
                "screen_index": idx,
                "scroll_top": scroll_top,
                "scroll_height": scroll_height,
                "client_height": client_height,
                "image_path": img_path,
                "ocr_json": json_path,
                "ocr_txt": txt_path,
                "ocr_joined_text": " | ".join(plain_texts)
            })

            # 到底部就停
            if scroll_top + client_height >= scroll_height - 5:
                print("[INFO] Reached bottom.")
                break

            new_scroll_top = scroll_one_page(driver, container)
            time.sleep(0.8)
            warm_up_visible_region(driver, container)
            time.sleep(0.4)

            new_info = get_container_info(driver, container)
            new_scroll_top = int(new_info["scrollTop"])

            if new_scroll_top == last_scroll_top or new_scroll_top == scroll_top:
                print("[INFO] Scroll position not changed, stopping.")
                break

            last_scroll_top = new_scroll_top

        if 'report_date' not in locals():
            report_date = local_today
        if 'OUTPUT_DIR' not in locals():
            output_paths = build_output_paths(report_date)
            OUTPUT_DIR = output_paths["output_dir"]
            SCREEN_DIR = output_paths["screen_dir"]
            OCR_JSON_DIR = output_paths["ocr_json_dir"]
            OCR_TXT_DIR = output_paths["ocr_txt_dir"]
        df = pd.DataFrame(records)
        csv_path = os.path.join(OUTPUT_DIR, f"mwr_ocr_screens_{report_date}.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[SAVED] {csv_path}")

        if extracted_rows:
            table_df = pd.DataFrame(extracted_rows)

            # 只按真实数据列去重（忽略 screen_index / crawl_time / row_order）
            dedup_cols = [
                "流域",
                "行政区划",
                "河名",
                "库名",
                "时间",
                "库水位(米)",
                "日变幅(米)"
            ]

            # 有些边界行可能缺列，先确保列存在
            for col in dedup_cols:
                if col not in table_df.columns:
                    table_df[col] = ""

            table_df = table_df.drop_duplicates(subset=dedup_cols, keep="first")

            table_csv_path = os.path.join(OUTPUT_DIR, f"mwr_ocr_table_{report_date}.csv")
            table_df.to_csv(table_csv_path, index=False, encoding="utf-8-sig")
            print(f"[SAVED] {table_csv_path}")

    finally:
        driver.quit()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise
