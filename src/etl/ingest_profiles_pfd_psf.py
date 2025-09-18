import pandas as pd
from pathlib import Path
import re

OUT = Path("data/interim"); OUT.mkdir(exist_ok=True, parents=True)

def tidy_wide_time(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    time_cols = [c for c in df.columns if re.fullmatch(r"\d{4}(-\d{2}|Q[1-4])?$", str(c).strip())]
    if not time_cols:
        time_cols = [c for c in df.columns if re.fullmatch(r"\d{4}$", str(c).strip())]
    id_vars = [c for c in df.columns if c not in time_cols][:2]
    long = df.melt(id_vars=id_vars, value_vars=time_cols, var_name="period", value_name="value")
    return long

# 1) OBR EFO monthly profiles
profiles_path = Path("data/raw/OBR - EFO March 2025 (core forecast + detailed tables)/March_2025_EFO_monthly_profiles.xlsx")
if profiles_path.exists():
    frames = []
    xls = pd.ExcelFile(profiles_path)
    for sh in xls.sheet_names:
        try:
            df = xls.parse(sh, header=0)
        except Exception:
            continue
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty: continue
        long = tidy_wide_time(df)
        long["vintage"] = "OBR_EFO_2025-03"
        long["source_file"] = profiles_path.name
        long["source_sheet"] = sh
        long["frequency"] = long["period"].astype(str).map(
            lambda s: "monthly" if re.fullmatch(r"\d{4}-\d{2}", s) else ("annual" if re.fullmatch(r"\d{4}", s) else "unknown")
        )
        long["variable"] = "monthly_profile"
        frames.append(long)
    if frames:
        pd.concat(frames, ignore_index=True).to_parquet(OUT / "obr_efo_2025-03_monthly_profiles.parquet", index=False)

# 2) OBR Public Finances Databank (your file name)
pfd_path = Path("data/raw/ONS - Public finances & GDP/PSF_aggregates_databank_Aug-3 (1).xlsx")
if pfd_path.exists():
    frames = []
    xls = pd.ExcelFile(pfd_path)
    for sh in xls.sheet_names:
        try:
            df = xls.parse(sh, header=0)
        except Exception:
            continue
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty: continue
        long = tidy_wide_time(df)
        long["vintage"] = "OBR_PFD_2025-08"
        long["source_file"] = pfd_path.name
        long["source_sheet"] = sh
        long["frequency"] = long["period"].astype(str).map(
            lambda s: "monthly" if re.fullmatch(r"\d{4}-\d{2}", s) else ("annual" if re.fullmatch(r"\d{4}", s) else "unknown")
        )
        long["variable"] = "pfd"
        frames.append(long)
    if frames:
        pd.concat(frames, ignore_index=True).to_parquet(OUT / "obr_pfd_2025-08.parquet", index=False)

# 3) ONS PSF time-series workbook
psf_path = Path("data/raw/ONS - Public finances & GDP/Public Sector Finance.xlsx")
if psf_path.exists():
    frames = []
    xls = pd.ExcelFile(psf_path)
    for sh in xls.sheet_names:
        try:
            df = xls.parse(sh, header=0)
        except Exception:
            continue
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty: continue
        long = tidy_wide_time(df)
        long["vintage"] = "ONS_PSF_master"
        long["source_file"] = psf_path.name
        long["source_sheet"] = sh
        long["frequency"] = long["period"].astype(str).map(
            lambda s: "monthly" if re.fullmatch(r"\d{4}-\d{2}", s) else ("annual" if re.fullmatch(r"\d{4}", s) else "unknown")
        )
        long["variable"] = "psf"
        frames.append(long)
    if frames:
        pd.concat(frames, ignore_index=True).to_parquet(OUT / "ons_psf_master.parquet", index=False)

print("Done.")
