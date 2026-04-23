

import os
import sys
import time
import json
import socket
import traceback
import subprocess
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
# Output root: honor $OUTPUT_DIR so the scraper can write into the repo's
# data/china/mwr/ layout on a server, while still defaulting to a sibling
# folder for local dev runs.
OUTPUT_ROOT_DIR = Path(os.environ.get("OUTPUT_DIR", str(BASE_DIR / "output_mwr_ocr"))).resolve()
MAIN_SCRIPT = BASE_DIR / "mwr_ocr_screens.py"
RECOVER_SCRIPT = BASE_DIR / "mwr_ocr_recover_failed_screens.py"
LOCK_FILE = OUTPUT_ROOT_DIR / ".grab_data.lock"
RUN_LOG_DIR = OUTPUT_ROOT_DIR / "run_logs"

OUTPUT_ROOT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def find_python_executable() -> str:
    """
    优先使用当前 conda/env 的 python，避免定时任务里环境不一致。
    """
    return sys.executable


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def acquire_lock() -> None:
    if LOCK_FILE.exists():
        try:
            old = json.loads(LOCK_FILE.read_text(encoding="utf-8"))
        except Exception:
            old = {"raw": LOCK_FILE.read_text(encoding="utf-8", errors="ignore")}
        raise RuntimeError(f"检测到已有运行中的任务或残留锁文件: {old}")

    lock_info = {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "start_time": now_str(),
    }
    write_json(LOCK_FILE, lock_info)


def release_lock() -> None:
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()


def latest_date_dir() -> Path | None:
    if not OUTPUT_ROOT_DIR.exists():
        return None
    date_dirs = [p for p in OUTPUT_ROOT_DIR.iterdir() if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-"]
    if not date_dirs:
        return None
    return sorted(date_dirs)[-1]


def run_one_script(script_path: Path, step_name: str) -> dict:
    python_exec = find_python_executable()
    start = time.time()
    start_ts = now_str()
    log_path = RUN_LOG_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{step_name}.log"

    with open(log_path, "w", encoding="utf-8") as logf:
        logf.write(f"[{start_ts}] START {step_name}\n")
        logf.write(f"python: {python_exec}\n")
        logf.write(f"script: {script_path}\n\n")
        logf.flush()

        process = subprocess.run(
            [python_exec, str(script_path)],
            cwd=str(BASE_DIR),
            stdout=logf,
            stderr=subprocess.STDOUT,
            text=True,
        )

    end = time.time()
    return {
        "step": step_name,
        "script": str(script_path),
        "returncode": process.returncode,
        "started_at": start_ts,
        "finished_at": now_str(),
        "duration_seconds": round(end - start, 2),
        "log_path": str(log_path),
    }


def build_summary(main_result: dict, recover_result: dict | None) -> dict:
    date_dir = latest_date_dir()
    report = {
        "run_time": now_str(),
        "base_dir": str(BASE_DIR),
        "output_root_dir": str(OUTPUT_ROOT_DIR),
        "latest_date_dir": str(date_dir) if date_dir else None,
        "main_result": main_result,
        "recover_result": recover_result,
    }

    if date_dir:
        report["expected_outputs"] = {
            "main_table": str(date_dir / f"mwr_ocr_table_{date_dir.name}.csv"),
            "screen_table": str(date_dir / f"mwr_ocr_screens_{date_dir.name}.csv"),
            "full_table": str(date_dir / f"mwr_ocr_full_table_{date_dir.name}.csv"),
            "recover_dir": str(date_dir / "recover_failed"),
        }

    return report


def main() -> None:
    acquire_lock()
    summary_path = RUN_LOG_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_summary.json"

    try:
        if not MAIN_SCRIPT.exists():
            raise FileNotFoundError(f"找不到主脚本: {MAIN_SCRIPT}")
        if not RECOVER_SCRIPT.exists():
            raise FileNotFoundError(f"找不到补救脚本: {RECOVER_SCRIPT}")

        print(f"[{now_str()}] 开始运行主抓取脚本: {MAIN_SCRIPT}")
        main_result = run_one_script(MAIN_SCRIPT, "01_main")
        print(f"[{now_str()}] 主抓取脚本结束，returncode={main_result['returncode']}")

        recover_result = None
        if main_result["returncode"] == 0:
            print(f"[{now_str()}] 开始运行补救脚本: {RECOVER_SCRIPT}")
            recover_result = run_one_script(RECOVER_SCRIPT, "02_recover")
            print(f"[{now_str()}] 补救脚本结束，returncode={recover_result['returncode']}")
        else:
            print(f"[{now_str()}] 主抓取失败，跳过补救脚本")

        summary = build_summary(main_result, recover_result)
        write_json(summary_path, summary)

        if main_result["returncode"] != 0:
            raise RuntimeError(f"主抓取脚本失败，详见日志: {main_result['log_path']}")
        if recover_result is not None and recover_result["returncode"] != 0:
            raise RuntimeError(f"补救脚本失败，详见日志: {recover_result['log_path']}")

        print(f"[{now_str()}] 全流程完成，摘要文件: {summary_path}")

    except Exception as e:
        error_summary = {
            "run_time": now_str(),
            "error": str(e),
            "traceback": traceback.format_exc(),
        }
        write_json(summary_path, error_summary)
        print(f"[{now_str()}] 任务失败: {e}")
        raise
    finally:
        release_lock()


if __name__ == "__main__":
    main()