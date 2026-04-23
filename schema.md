# Global Reservoir Dataset — 数据输出约定（Schema）

> 从 2026-04 开始，**新抓取的国家**按本约定输出。老国家不强制迁移，等用到时再说。
> 统一格式 CSV、UTF-8 with BOM、逗号分隔、ISO 日期（`YYYY-MM-DD`）。
> 坐标一律 **WGS84 十进制度**（`lat`, `lon`，南/西为负）。
>
> **变量名 / 单位原则**：抓取阶段**不强行统一**。源站给什么就抓什么，列名保留原文；
> 看不懂的在旁边加圆括号通俗解释，单位也写在括号里。比如 Argentina 的做法：
> `Altura hidrométrica (gage height) | metros (meters)`。
> 统一成枚举/标准单位是**下游一步**的事（之后写一个 normalize 脚本做），现在脚本别自己换算。

## 0. 顶层组织：**现场观测 vs 遥感数据**（重要）

数据质量差异很大，必须在**目录层级**就能一眼区分：

```
data/
├── <country>/<source>/           # 🟢 现场观测 / 仪器 / 人工记录（in-situ）
│   ├── philippines/pagasa/
│   ├── japan/opengov/
│   ├── thailand/rid/
│   └── ...
└── remote_sensing/               # 🛰️ 遥感派生数据（卫星测高、影像反算等）
    ├── stimson_mekong/           # 多国共用的 RS 产品放这里，不按国家拆
    ├── g_realm/                  # NASA/CNES 雷达测高
    └── hydroweb/                 # LEGOS 水位库
```

**规则**：
- `data/<country>/` = **现场观测**（gauge / telemetry / 人工读数 / 管理局公报），优先采用。
- `data/remote_sensing/` = **遥感派生**（卫星测高估算水位、SAR/光学反演库容等），多国共用数据源放这里。
- 一个源如果**同时**提供现场与遥感，按**主导方式**归类；metadata 里再按水库单独标注（见下）。
- `metadata` CSV 里**必须**有 `data_type` 列，取值：
  - `in_situ` — 仪器/人工观测，原始测量
  - `remote_sensing` — 卫星/航拍反演（哪怕精度再高也归这里）
  - `mixed` — 同一水库现场+遥感融合
  - `model` — 水文模型输出

## 1. 每个国家的目录结构

```
<Country>/
├── <script>.py                                 # 抓取脚本
├── <country>_outputs/
│   ├── metadata/
│   │   ├── <country>_reservoirs.csv            # ★ 必有：一行一个水库/站
│   │   └── <country>_variables.csv             # 可选：变量代码 → 名称/单位
│   ├── timeseries/
│   │   ├── daily/                              # 日更新源站 → 每天一个文件
│   │   │   └── <country>_timeseries_YYYY-MM-DD.csv
│   │   ├── weekly/                             # 周更新源站 → 每周一个文件
│   │   │   └── <country>_timeseries_YYYY-MM-DD.csv   # 日期=公报日
│   │   ├── monthly/                            # 月更新源站 → 每月一个文件
│   │   │   └── <country>_timeseries_YYYY-MM.csv
│   │   └── <country>_timeseries_long.csv       # 可选：聚合后的长表
│   └── raw/                                    # 可选：PDF / HTML / 截图等原始件
└── run_logs/                                   # 可选：定时任务日志、summary.json
```

- 按日/周/月哪种就只建哪种目录，别三个都建空的。
- 聚合长表按需生成（通常从 snapshot 文件拼出来），大到内存不够就只留 snapshot 不生成它。

## 2. metadata 表：`<country>_reservoirs.csv`

**一行 = 一个水库/站**。列名规范（缺失的留空，不要删列）：

| 列名 | 必填 | 说明 |
|---|---|---|
| `reservoir_id` | ✅ | 主键。优先用源站自己的 ID；源站没给就用 `<country>_<序号>`（如 `MY_001`），长期稳定不可变 |
| `reservoir_name` | ✅ | 原语言名称（中文/泰语/西班牙语…都按原文） |
| `reservoir_name_en` |  | 有英文就填 |
| `country` | ✅ | 英文全名 `Thailand` / `China` / `South Africa` … |
| `admin_unit` |  | 省/州/地区，按源站原文 |
| `river` |  | 河流名 |
| `basin` |  | 流域名 |
| `lat` | ✅ | 十进制度 WGS84。源站只有度分秒就换算 |
| `lon` | ✅ | 同上 |
| `capacity_total (<unit>)` |  | 总库容，列名括号里写源站给的单位原文，比如 `capacity_total (mcm)` / `capacity_total (10^6 m^3)` / `capacity_total (亿立方米)`。**不要换算**，原单位抓进来 |
| `dead_storage (<unit>)` |  | 死库容，同上 |
| `frl (<unit>)` |  | 正常蓄水位 full reservoir level，通常米 |
| `dam_height (<unit>)` |  | 坝高，通常米 |
| `year_built` |  | 建成年份 |
| `main_use` |  | `irrigation` / `hydroelectricity` / `water_supply` / `flood_control` / `navigation` / `multipurpose` 等 |
| `source_agency` | ✅ | 数据来源机构缩写：`RID` / `INA` / `MWR` / `CWC` / `DWS` / `BOM` / `WRA` / `CAWATER` … |
| `source_url` | ✅ | 这条记录的抓取 URL。**只在 metadata 出现，时序表不再重复** |
| `data_type` | ✅ | `in_situ` / `remote_sensing` / `mixed` / `model`（见 §0） |
| `last_updated` | ✅ | 本行最近一次抓取时间，`YYYY-MM-DD HH:MM:SS` |

**允许在后面加国家专属列**（比如 Thailand 的 `rid_office`、South Africa 的 `wma`），但前面的列顺序别动。

## 3. 时序表：snapshot 文件（Thailand 模式）

对于**日/周/月更新**的源站，一次运行写一个文件，文件名带日期，内容**自包含**（站元信息+当期观测都在一行里），方便肉眼打开任何一天看当天全国情况。

### 文件命名
- 日：`<country>_timeseries_2026-04-21.csv`
- 周：`<country>_timeseries_2026-04-19.csv`（日期 = 公报日 / 周末日，脚本里注释清楚取哪天）
- 月：`<country>_timeseries_2026-04.csv`

### 列规范（宽表，一行 = 某水库某天的所有观测）

**固定三列（必有，放最前面）**：

| 列名 | 说明 |
|---|---|
| `reservoir_id` | 外键 → metadata |
| `reservoir_name` | 冗余写入，方便肉眼查看 |
| `date` | `YYYY-MM-DD`（月数据用 `YYYY-MM-01`） |

**观测列：源站给什么抓什么，不强行统一命名/单位**
- 列名保留源站原文；不好理解的**后面加圆括号英译或通俗名**。例：
  - 中文 `库水位` → `库水位 (water level)`
  - 泰语 `ระดับน้ำ` → `ระดับน้ำ (water level)`
  - 西语 `Altura hidrométrica` → `Altura hidrométrica (gage height)`
- **单位也写在列名的括号里**，跟名字连在一起或分成两个括号都行：
  - `库水位 (water level, m)`
  - `storage_current (百万立方米, 10^6 m^3)`
  - `inflow_daily (m^3/s)`
- 数值格子里**不带单位**（比如不要写 `"35.2 m"`，只写 `35.2`）
- 源站没给的字段留空，别新造也别填 `--`

**不要放进 snapshot 的东西**：
- lat / lon / capacity / river 等元信息 —— metadata 里有就够了。Thailand 现在是冗余写的，以后重抓时去掉。

### 多变量粒度不一致怎么办？
- 例：水位是日级、inflow 是瞬时 15 分钟 → 放在 `timeseries/daily/` 的 snapshot 里只保留**日均/日累计**值；瞬时/亚日数据另存 `timeseries/raw_subdaily/<station_id>/YYYY-MM.csv` 之类。
- 例：源站只给月度数据（中亚 cawater）→ 放 `timeseries/monthly/`，列里只填 `storage_mcm` / `inflow_mcm` / `outflow_mcm`，日级列留空。

## 4. 可选的聚合长表：`<country>_timeseries_long.csv`

从 snapshot 文件 melt 出来的瘦表，方便合并分析：

| 列名 | 说明 |
|---|---|
| `reservoir_id` | |
| `date` | |
| `variable_original` | 源站原列名（含单位括号），例：`库水位 (water level, m)` |
| `variable_gloss` | 可选，通俗/英文解释，例：`water level` |
| `unit` | 单位，抄列名括号里的那个（`m` / `10^6 m^3` / `%` …），**不换算** |
| `value` | 数值 |
| `quality_flag` | 可选 |

生成逻辑写在脚本里，跑完 snapshot 再跑一遍聚合。数据量大到 RAM 吃紧就按年切：`<country>_timeseries_long_2025.csv`, `_2026.csv`。

## 5. 标准化（未来一步，现在不做）

抓取层不做标准化。以后需要跨国对比时，单独写一个 `normalize.py`：
- 读各国 `<country>_timeseries_long.csv`
- 用一张手工维护的 `variable_map.csv`（`country, variable_original → standard_variable, unit_factor`）做映射 + 单位换算
- 产出统一 schema 的 `global_timeseries_long.csv`（这时才会出现类似 `water_level_m` / `storage_mcm` 这种标准化名字）

这样抓取脚本不用碰单位换算，也不会因为某国新增变量/改单位就推翻已有逻辑。

## 6. 编码、分隔符、日期

- **编码**：UTF-8 with BOM（Excel on Windows 才不乱码；Mac 也正常）
- **分隔符**：半角逗号
- **引号**：值里含逗号 / 换行 / 引号时用双引号包起来，内部双引号翻倍转义（pandas `to_csv` 默认行为）
- **日期**：ISO 8601，`YYYY-MM-DD`（月级就用 `YYYY-MM-01` 补齐）
- **时间戳**（如 `last_updated`）：`YYYY-MM-DD HH:MM:SS`，全用本地时间，必要时加 `+HH:MM` 时区
- **小数**：点号，不用千分位
- **缺失值**：空字符串（不是 `NA` / `--` / `-999`），源站给的 `--` 要在脚本里 clean 掉
- **布尔**：`True` / `False`

## 7. 脚本侧要做的事

- 每次运行写 `run_logs/<timestamp>_summary.json`（跟 China 一样），至少包含：运行时间、返回码、产出文件路径、报错 traceback
- 增量抓取：先读 `timeseries/<granularity>/` 已有文件，跳过已完成日期；失败的写 `retry_queue.csv`
- metadata 不必每次重写，独立有个 `refresh_metadata.py` 或在主脚本里加 `--refresh-metadata` flag
- source_url 变了（比如 API 迁新地址）时在 metadata 里更新 `source_url` + `last_updated`

## 8. 新国家上手步骤

1. 建目录 `<Country>/`
2. 先跑一次小样本，输出 `metadata/<country>_reservoirs.csv` 对照本文档核查列全了没
3. 跑一天/一周的 snapshot，打开 CSV 肉眼看格式
4. 把脚本加进定时任务（本地 launchd / GitHub Actions / VPS）
5. 写一个本国专属的 `README.md` 记录源站 URL、抓取节奏、已知坑（比如哪些字段缺、单位换算规则）

---
*最后更新：2026-04-21*
