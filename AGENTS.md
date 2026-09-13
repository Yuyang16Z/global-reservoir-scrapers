# AGENTS.md · Windowed Source Archival Subsystem

> 本文件适用于 `Yuyang16Z/global-reservoir-scrapers` 仓库。它是全球水库本地项目的一个子系统，不是完整项目本身。若上层工作区可用，先读 `../AGENTS.md`、`../PROJECT_STATUS.md` 和 `../docs/PROJECT_MAP.md`。

同目录 `CLAUDE.md` 只用于 Claude Code 自动发现，并指向本文件；不在两处维护重复规则。

## 一、仓库使命与非目标

本仓库是 **ephemeral/windowed archival subsystem**：定时捕获官方网站中会消失、缩短或被新公报覆盖的水库观测，并保存可追溯的历史证据。

本仓库不是：

- 全部国家和来源的研究工作区；
- 完整历史档案的默认镜像或备份站；
- 跨国字段/单位标准化层；
- `Formatted reservoir data/` 教授交付层；
- 可以被仓库内运行或数据数量替代的项目总状态来源。

仓库中已有的静态 metadata、河站 cross-reference 或永久归档的历史例外为兼容资产，不构成新来源的准入先例。不因本次 Agent 层调整删除或改动这些现有资产。

## 二、启动顺序

1. 检查仓库根、当前分支、工作树状态和 `origin/main` 差异；
2. 读 `README.md`、`WINDOWED_SOURCE_POLICY.md`、`config/windowed_sources.json` 和 `DATA_LICENCE_NOTICE.md`；
3. 读 `schema.md` 和目标来源的 README、运行摘要及数据证据；
4. 使用 `agent-skills/windowed-source-archival/SKILL.md` 进行新来源准入或归档运维；
5. 若任务跨到本地国家研究、格式化交付或项目汇报，返回上层工作区并使用完整 `reservoir-data-pipeline` Skill。

不将完整 `reservoir-data-pipeline` Skill、顶层 `agent-evals/`、交付验证或会议汇报流程复制进本仓库。

易变的分支、数量和新鲜度只记录在当前证据或上层 `PROJECT_STATUS.md`，不复制进本长期规则文件。

## 三、新来源准入门禁

任何新来源在编写工作流或决定 GitHub 部署前，必须在来源 Profile 中分类 retention：

| retention class | 本仓库决定 |
|---|---|
| `rolling_window` | 符合归档使命；通过证据、许可和运维门禁后可准入 |
| `current_snapshot` | 符合归档使命；必须保存不可覆盖或可校验的快照 |
| `overwrite_prone` | 符合归档使命；必须有发布日期、重试、回看和恢复余量 |
| `permanent_archive` | 默认拒绝准入；留在上层本地 `resovoir data/<country>/` |
| `unknown_review` | 不部署；返回调查并记录未决问题 |

`permanent_archive` 只有在存在与“数据会消失”不同的明确运维需求、记录理由并获得人工决定时才能例外准入。

准入条件还包括：官方性和对象范围证据、观测/公报日期语义、发布节奏、保留窗口、直接历史查询能力、许可证据、稳定合并键、重叠回看、恢复余量和失败可见性。详细判据以 `WINDOWED_SOURCE_POLICY.md` 和窄 Skill 为准。

## 四、抓取与归档硬规则

1. **来源语义优先。** 保存原始字段、单位和官方观测/公报日期；不在本层创造标准化合计。
2. **不制造历史。** 不插值、复制最近值、猜测缺报或把抓取日期写成源日期。
3. **证据可追溯。** 保存来源 URL、抓取时间、raw/校验信息和 run summary；不覆盖唯一历史证据。
4. **幂等且能恢复。** 按稳定对象、观测日期和来源序列合并；重跑不产生重复，回看窗口能覆盖错过一次调度。
5. **失败可见。** 绿色工作流不等于数据新鲜；检查源日期、行/对象数、连续 no-data、上游停更和解析告警。
6. **审计范围不得夸大。** `scripts/audit_windowed_sources.py` 审计已登记来源的一致性，不自动证明仓库内所有符合条件的来源都已登记。

## 五、许可与公开暴露

自动化不会改变源站权利。必须读 `DATA_LICENCE_NOTICE.md`：本仓库已记录一项 2026-08-04 owner decision，即现有定时收集继续，但仓库公开导致数据实际 world-readable，与多个来源的 `undeclared_review`/`mixed_review` 存在明示张力。

本次 Agent 层调整不停掉或改写现有运行。但对任何新来源、新数据路径或新的公开暴露，Agent 不得将 `undeclared_review`、`restricted_use`、`mixed_review` 或 `prohibited` 自行升级为可公开归档；必须停在人工审核点。“定时收集”、“world-readable”和“允许再发布”是三个不同事实。

## 六、目录与验证入口

- 来源代码：`scrapers/<country>/<source>/`
- 归档输出：`data/<country>/<source>/`
- 定时任务：`.github/workflows/`
- 短窗口注册表：`config/windowed_sources.json`
- 治理与监控：`scripts/`
- 仓库级狭 Skill：`agent-skills/windowed-source-archival/`
- 仓库级 Agent 评测：`agent-evals/windowed-source-archival-evaluation.md`

与风险相称地运行：

```bash
python3 scripts/audit_windowed_sources.py
python3 scripts/monitor_source_freshness.py
python3 -m unittest discover -s tests -p 'test_*.py'
```

修改窄 Skill 后，使用 `skill-creator` 提供的 `quick_validate.py` 做结构校验；不在仓库文档中硬编码某一用户的解释器路径。来源专属测试使用 fixture 或隔离输出目录，不用正式 `data/` 做首次实验。

## 七、Git 与外部操作

- 保留用户已有变更；从当前远端基线的干净工作树复现，只移植明确路径。
- 不批量暂存，不为消除 diff 而覆盖或删除历史 raw 证据。
- 未经用户明确要求，不 push、不创建/合并 PR、不触发工作流、不修改 GitHub 数据或密钥。
- 对新的公开数据暴露和任何不清晰许可，必须先获得人工决定。

## 八、完成与交接

每个结果必须分开报告：

1. 本地代码与测试；
2. GitHub 调度或其他外部部署；
3. 数据新鲜度和归档覆盖；
4. 许可和公开暴露状态；
5. 格式化交付状态。

使用 `verified`、`partial`、`blocked`、`regressed` 或 `unverified`。“已归档”不等于“已交付”；除非实际进入上层 `Formatted reservoir data/` 并通过它的 Schema 和质量审计，否则交付状态必须保持独立或 `unverified`。
