import pandas as pd
from pathlib import Path
import re

OUT = Path("data/interim"); OUT.mkdir(parents=True, exist_ok=True)

def longify(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    time_cols = [c for c in df.columns if re.fullmatch(r"\d{4}(-\d{2})?$", str(c))]
    if not time_cols:
        time_cols = [c for c in df.columns if re.fullmatch(r"\d{4}$", str(c))]
    id_vars = [c for c in df.columns if c not in time_cols][:2]
    return df.melt(id_vars=id_vars, value_vars=time_cols, var_name="period", value_name="value")

# 1) ONS mm23 (CPI/CPIH)
mm23 = Path("data/raw/ONS - Prices (inflation)/mm23.xlsx")
if mm23.exists():
    xls = pd.ExcelFile(mm23)
    for sh in xls.sheet_names:
        try:
            df = xls.parse(sh, header=0)
        except Exception:
            continue
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty: continue
        long = longify(df)
        long["vintage"]="ONS_mm23"
        long["variable"]="prices"
        long["source_file"]=mm23.name
        long["source_sheet"]=sh
        long.to_parquet(OUT / f"ons_mm23_{sh.replace(' ','_')}.parquet", index=False)

# 2) HMT GDP deflators
gdp_defl = Path("data/raw/HMT - Spending, balance sheet, deflators/GDP_Deflators_Qtrly_National_Accounts_June_2025_update.xlsx")
if gdp_defl.exists():
    frames=[]
    xls = pd.ExcelFile(gdp_defl)
    for sh in xls.sheet_names:
        try:
            df = xls.parse(sh, header=0)
        except Exception:
            continue
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty: continue
        long = longify(df)
        long["vintage"]="HMT_GDP_deflators_2025-06"
        long["variable"]="gdp_deflator"
        long["source_file"]=gdp_defl.name
        long["source_sheet"]=sh
        frames.append(long)
    if frames:
        pd.concat(frames, ignore_index=True).to_parquet(OUT / "hmt_gdp_deflators_2025-06.parquet", index=False)

# 3) BoE quoted rates (CSV)
boe_rates = Path("data/raw/BoE - Rates and Yields/Quoted Rates Bank of England Database.csv")
if boe_rates.exists():
    try:
        df = pd.read_csv(boe_rates)
    except Exception:
        df = pd.read_csv(boe_rates, encoding="latin-1")
    df.to_parquet(OUT / "boe_quoted_rates.parquet", index=False)

print("Done.")
