# Malaysia 水库数据

## 当前覆盖（v1，2026-04-21）

### ✅ LUAS IWRIMS — Selangor 州，8 座水坝 + 1 座拦河坝
- 脚本: `malaysia_luas_scraper.py`
- 输出: `malaysia_luas_outputs/`
- 源: <https://iwrims.luas.gov.my/getMapData_JSON.cfm?data=damstation>
- 无需登录的公开 JSON API
- **抓到**: `WATER_LEVEL` 水位 / `STORAGE` 当前库容 / `STORAGE_PERCENT` 蓄水百分比 / `RELEASE_MLD` 释放量（出流，单位 mld=百万升/天）/ `SPILL` 溢洪 / `RAIN` 降雨量（mm） / 经纬度 / 正常蓄水位 / 死库容 / 库容上限 / 集水面积
- **抓不到**: inflow（源站本身不提供入流）；历史数据（源站只给实时快照，需要每日轮询累积）
- 水坝：Batu, Klang Gates, Kolam Takungan Sg. Labu, Langat, Semenyih, Sg Selangor, Sg Tinggi, Tasik Subang
- 拦河坝：Bestari Jaya

### ❌ 已探查但当前没用上的源
- **`hydroportal.water.gov.my`**（旧脚本 `grab_malaysia_did.py` 的目标，Aquarius WebPortal）：`SearchLocations` 接口返回空，估计需要登录。已放弃。
- **MyWater Portal `SumberAir/Empangan.aspx`**：只有 16 座 JPS 水坝的**静态**元数据（高度/坝顶长度/库容等），无 timeseries。可以作为补充 metadata 源，以后有需要再合并。
- **Public Infobanjir (`publicinfobanjir.water.gov.my/aras-air`)**：主要是河道水位站（200+），不是水库。如需要河道数据再抓。

### 🔜 待做（TODO）
- **Sarawak iHydro** (`ihydro.sarawak.gov.my`): 覆盖 Bakun / Murum / Batang Ai 等大型水电站，页面是 SPA，需要浏览器抓取或找后台 API
- **TNB 水电坝**（Kenyir, Pergau, Temengor, Sultan Mahmud 等）：TNB 没有公开 API，可能需要联系或爬新闻稿
- **其他州 InfoBanjir 门户**: Pahang / Johor / Kelantan / Perak / Kedah / Penang / Terengganu / Sabah 的 JPS 门户，大多只给河道水位
- **Air Selangor、Ranhill、PBAPP 等水务公司**：可能有水库运行数据但需要确认公开性
- 整合 MyWater JPS 16 座水坝的静态 metadata 到 `metadata/` 下作为附加 reference

## 运行方式
```bash
cd "/Users/andy/Desktop/work/resovoir data/Malaysia"
python malaysia_luas_scraper.py
```
无需参数。每次运行会：
1. 拉一次 damstation + barrage 的 JSON（保存到 `raw/`）
2. upsert 写 `metadata/malaysia_luas_reservoirs.csv`
3. 按观测日期写 `timeseries/daily/malaysia_luas_timeseries_YYYY-MM-DD.csv`
4. 写一份 `run_logs/*_summary.json`

## 定时建议
LUAS 的数据一天更新 1~2 次（多数水坝早上 6–8 点出新），建议每天跑 2 次（早 9 点 + 晚 9 点），早的那次抓当天观测，晚的那次确保补上当天后更新的站。

launchd plist 示例（还没部署，等决定了放哪再启用）：
```xml
<key>StartCalendarInterval</key>
<array>
  <dict><key>Hour</key><integer>9</integer><key>Minute</key><integer>0</integer></dict>
  <dict><key>Hour</key><integer>21</integer><key>Minute</key><integer>0</integer></dict>
</array>
```

## 旧脚本
`grab_malaysia_did.py` + `malaysia_did_outputs/`：基于 Aquarius WebPortal，目标站已经不响应，保留作存档，不再维护。
