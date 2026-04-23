# China MWR (水利部) 全国大型水库实时水情

Scrapes <http://xxfb.mwr.cn/sq_dxsk.html?v=1.0> —— 水利部发布的全国大型水库实时水情页面。

数据是 JavaScript 渲染的动态表格，没有公开 API，只能用 **Selenium + 截图 + OCR** 的方式抓。

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
cd scrapers/china/mwr
python grab_data.py
# 数据写到 ./output_mwr_ocr/<date>/
```

### 服务器（headless + 写到 repo）

```bash
OUTPUT_DIR=$REPO/data/china/mwr \
MWR_HEADLESS=1 \
python scrapers/china/mwr/grab_data.py
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
| `OUTPUT_DIR` | `./output_mwr_ocr` | 输出根目录。服务器上指向 `$REPO/data/china/mwr/` |
| `MWR_HEADLESS` | `1` | `0` = 看得到浏览器（本地调试）；`1` = 无头（服务器必须） |

## 输出结构

```
data/china/mwr/
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
