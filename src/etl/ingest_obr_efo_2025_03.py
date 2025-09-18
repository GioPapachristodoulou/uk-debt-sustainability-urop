import pandas as pd
from pathlib import Path
import re

RAW_DIR = Path("data/raw/OBR - EFO March 2025 (core forecast + detailed tables)")
OUT_DIR = Path("data/interim")
OUT_DIR.mkdir(parents=True, exist_ok=True)

VINTAGE = "OBR_EFO_2025-03"

# Map each workbook to how we want to label its variables by default
BOOKS = {
    "Aggregates_Detailed_forecast_tables_March_2025.xlsx": "aggregates",
    "Economy_Detailed_forecast_tables_March_2025.xlsx": "economy",
    "Expenditure_Detailed_forecast_tables_March_2025.xlsx": "expenditure",
    "Receipts_Detailed_forecast_tables_March_2025.xlsx": "receipts",
    "Policy_Detailed_forecast_tables_March_2025.xlsx": "policy",
    "Debt_interest_Detailed_forecast_tables_March_2025.xlsx": "debt_interest",
    "Annex_A_charts_and_tables_March_2025.xlsx": "annex_a",
    "Executive_summary_charts_and_tables_March_2025.xlsx": "executive_summary",
    # Chapters:
    "Chapter_2_charts_and_tables_March_2025.xlsx": "ch2",
    "Chapter_3_charts_and_tables_March_2025.xlsx": "ch3",
    "Chapter_4_charts_and_tables_March_2025.xlsx": "ch4",
    "Chapter_5_charts_and_tables_March_2025.xlsx": "ch5",
    "Chapter_6_charts_and_tables_March_2025.xlsx": "ch6",
    "Chapter_7_charts_and_tables_March_2025.xlsx": "ch7",
}

# Helpers
def _tidy_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _longify(df: pd.DataFrame, id_vars) -> pd.DataFrame:
    # melt any wide-year tables into year,value
    value_cols = [c for c in df.columns if c not in id_vars]
    # filter to columns that look like years or periods
    keep = []
    for c in value_cols:
        sc = str(c).strip()
        if re.fullmatch(r"\d{4}", sc) or re.fullmatch(r"\d{4}-\d{2}", sc) or re.fullmatch(r"\d{4}Q[1-4]", sc):
            keep.append(c)
    value_cols = keep if keep else value_cols
    long = df.melt(id_vars=id_vars, value_vars=value_cols, var_name="period", value_name="value")
    return long

def _infer_frequency(period: str) -> str:
    if re.fullmatch(r"\d{4}$", period): return "annual"
    if re.fullmatch(r"\d{4}Q[1-4]$", period): return "quarterly"
    if re.fullmatch(r"\d{4}-\d{2}$", period): return "monthly"
    return "unknown"

records = []

for fname, block in BOOKS.items():
    fpath = RAW_DIR / fname
    if not fpath.exists():
        # skip silently; not all files may be present
        continue
    xls = pd.ExcelFile(fpath)
    for sheet in xls.sheet_names:
        try:
            df = xls.parse(sheet, header=0)
        except Exception:
            continue
        if df.empty:
            continue
        df = _tidy_columns(df)
        # heuristic: look for a first column that looks like a series/description
        first_col = df.columns[0]
        # Drop fully empty columns/rows
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty: 
            continue

        # Try to identify id columns
        id_vars = [df.columns[0]]
        # keep a second id col if it looks categorical
        if len(df.columns) > 1 and df.columns[1].lower() in {"series","descriptor","description","measure","sector","component","table"}:
            id_vars.append(df.columns[1])

        long = _longify(df, id_vars=id_vars)
        if long.empty:
            continue

        # annotate
        long["vintage"] = VINTAGE
        long["frequency"] = long["period"].astype(str).map(_infer_frequency)
        long["variable"] = block
        long["subvariable"] = sheet
        long["units"] = ""  # we will standardize units in a second pass
        long["source_file"] = fname
        long["source_sheet"] = sheet
        long["source_series_id"] = long[id_vars[0]].astype(str)

        # Clean numbers
        long["value"] = pd.to_numeric(long["value"], errors="coerce")

        # Keep core columns
        keep_cols = ["vintage","frequency","period","variable","subvariable","value","units","source_file","source_sheet","source_series_id"]
        long = long[keep_cols]
        records.append(long)

# Concatenate & write
if records:
    out = pd.concat(records, ignore_index=True)
    out.to_parquet(OUT_DIR / "obr_efo_2025-03_interim.parquet", index=False)
    # quick CSV sample for inspection
    out.head(200).to_csv(OUT_DIR / "obr_efo_2025-03_sample.csv", index=False)
    print("Wrote:", OUT_DIR / "obr_efo_2025-03_interim.parquet")
else:
    print("No sheets parsed — check file names/locations.")
