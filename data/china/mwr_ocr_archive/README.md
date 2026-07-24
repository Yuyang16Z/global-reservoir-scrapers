# China MWR OCR archive (read-only)

This directory preserves the retired Selenium + screenshot + OCR collection
for the Ministry of Water Resources (MWR) national large-reservoir dashboard.
It is an audit and historical-recovery archive, not an active scraper output.

## Status

- **Archived:** 2026-07-24
- **Observation/capture folders:** 85
- **Folder coverage:** 2026-03-26 through 2026-06-28
- **Active replacement:** [`../mwr_api/`](../mwr_api/)
- **Future writes:** only `data/china/mwr_api/` is supported

The GitHub Actions workflow
[`china_mwr_api.yml`](../../../.github/workflows/china_mwr_api.yml) runs the
API/font-decoder pipeline. No workflow writes to this archive.

## Retained files

Each dated folder may contain:

- `mwr_ocr_full_table_*.csv`: recovered full OCR table when available
- `mwr_ocr_table_*.csv`: regular OCR table
- `mwr_ocr_screens_*.csv`: screen-level OCR extraction
- `ocr_txt/*.txt`: raw OCR text retained for audit and decoder review
- `column_template.json`: OCR column-boundary template when available

Large, regenerable intermediate artefacts are intentionally not stored in
ordinary Git history:

- source/debug PNG screenshots
- `recover_failed/` debug output
- `ocr_json/` intermediate OCR engine output
- lock and local runtime files

Those excluded intermediates remain in the original local archive. The compact
CSV and text evidence needed to inspect the historical extraction is retained
here.

## Use

Do not point cron jobs, GitHub Actions, or new collection scripts at this
directory. Use:

```text
scrapers/china/mwr_api/china_mwr_api_scraper.py
data/china/mwr_api/
```

The OCR tables do not always contain stable reservoir IDs and may include OCR
errors. Any historical merge should preserve source provenance and record
mapping or correction decisions.
