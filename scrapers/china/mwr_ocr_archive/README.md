# China MWR OCR scraper archive (retired)

> **Retired 2026-07-24. Do not schedule or use this scraper for new data.**
> The supported collector is
> [`../mwr_api/china_mwr_api_scraper.py`](../mwr_api/china_mwr_api_scraper.py),
> and all future outputs belong in `data/china/mwr_api/`. This directory is
> retained only to reproduce and audit the historical OCR archive.

Scrapes <http://xxfb.mwr.cn/sq_dxsk.html?v=1.0> —— 水利部发布的全国大型水库实时水情页面。

这是旧版 **Selenium + 截图 + OCR** 抓取路径，保留作审计、回退和
`mwr_api` 字体解码训练数据。稳态 GitHub Actions 现在优先使用更轻量的
[`../mwr_api`](../mwr_api/) 方案：直接请求页面背后的公开 JSON 端点，再解码
源站 custom font 混淆。

## 字段

- `流域` / `行政区划` / `河名` / `库名`
- `时间` —— 源站时刻（08:00 报的头一天 24 小时统计）
- `库水位(米)`
- `日变幅(米)` —— 24h 水位变化

源站没有 inflow / outflow / 蓄水量，只能拿到 level + daily change。

## 脚本

| 文件 | 作用 |
|---|---|
| `mwr_ocr_screens.py` | Chrome 滚截图 + PaddleOCR 识别 + 拼表 |
| `mwr_ocr_recover_failed_screens.py` | 对 OCR 失败的截图重跑 + 用 KMeans 做行对齐修复 |
| `grab_data.py` | Orchestrator：串联上面两个，带锁文件防并发 |
| `run_and_commit.sh` | cron 用的外层 wrapper，跑完自动 `git commit && push` |
| `setup_oracle_vm.sh` | Oracle Ubuntu ARM VM 初始化（sudo 运行一次） |
| `setup_git_deploy_key.sh` | 在 VM 上配 GitHub deploy key（运行一次） |

## 运行

### 本地（调试用，能看到浏览器）

```bash
cd scrapers/china/mwr_ocr_archive
python grab_data.py
# 数据写到 ./output_mwr_ocr/<date>/
```

### 服务器（headless + 写到 repo）

```bash
OUTPUT_DIR=$REPO/data/china/mwr_ocr_archive \
MWR_HEADLESS=1 \
python scrapers/china/mwr_ocr_archive/grab_data.py
```

### Oracle Cloud 部署

完整步骤见 [`ORACLE_SETUP.md`](./ORACLE_SETUP.md)。大致：
1. 开 ARM A1.Flex VM（Always Free，2 OCPU / 12 GB）
2. `bash setup_oracle_vm.sh` 装环境
3. `bash setup_git_deploy_key.sh` 配 git push 权限
4. cron `0 12 * * *` 调 `run_and_commit.sh`（UTC 12:00 = 北京 20:00）

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `OUTPUT_DIR` | `./output_mwr_ocr` | 输出根目录。服务器上指向 `$REPO/data/china/mwr_ocr_archive/` |
| `MWR_HEADLESS` | `1` | `0` = 看得到浏览器（本地调试）；`1` = 无头（服务器必须） |

## 输出结构

```
data/china/mwr_ocr_archive/
├── <YYYY-MM-DD>/
│   ├── mwr_ocr_table_YYYY-MM-DD.csv        # ★ 主表，推回 repo
│   ├── mwr_ocr_screens_YYYY-MM-DD.csv      # 按截图拆分的明细
│   ├── mwr_ocr_full_table_YYYY-MM-DD.csv   # 合并修复结果后的完整表
│   ├── column_template.json
│   ├── ocr_txt/                            # 每张截图的 OCR 纯文本
│   ├── ocr_json/                           # 每张截图的 OCR 结构化结果（体积大，gitignore）
│   ├── screens/                            # 截图原图（体积大，gitignore）
│   └── recover_failed/                     # 修复中间产物（体积大，gitignore）
└── run_logs/
    └── cron_YYYYMMDDTHHMMSSZ.log
```

**提交到 repo 的**：CSV + `ocr_txt/` + `column_template.json`（~250 KB/day）
**只留在 VM 上的**：`screens/` + `ocr_json/` + `recover_failed/`（~10 MB/day 截图 PNG，走 `.gitignore`）

## 已知坑

- Chromium 在 Linux headless 下字体渲染跟 Mac 不同，首次部署要确认 OCR 识别率没崩。装了 `fonts-noto-cjk` 应该 OK
- PaddleOCR 首次运行要下模型 ~300 MB，会卡住几分钟
- 源站页面在北京时间 08:00 前后更新当天报表。北京 20:00 跑是为了确保数据稳定
- 锁文件 `.grab_data.lock` 在 `$OUTPUT_DIR` 下，异常退出可能残留，必要时手动删
