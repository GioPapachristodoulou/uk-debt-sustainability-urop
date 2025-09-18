import pandas as pd, re, yaml
from pathlib import Path

EFO = Path("data/interim/obr_efo_2025-03_interim.parquet")
MAP = Path("config/mapping.yml")
OUT = Path("data/processed"); OUT.mkdir(parents=True, exist_ok=True)

def norm_period(s):
    s=str(s).strip()
    if re.fullmatch(r"\d{4}$",s): return s,"annual"
    if re.fullmatch(r"\d{4}Q[1-4]$",s): return s,"quarterly"
    if re.fullmatch(r"\d{4}-\d{2}$",s): return s,"monthly"
    return s,"unknown"

def infer_units(canon, text):
    t=text.lower()
    if "% of gdp" in t or canon.endswith("_gdp"): return "percent_gdp"
    if any(k in t for k in ["£"," bn","billion"]) or canon.endswith("_bn_gbp"): return "bn_gbp"
    if "index" in t or canon.endswith("_index"): return "index"
    return ""

df = pd.read_parquet(EFO)
cfg = yaml.safe_load(MAP.read_text())
rows=[]
for m in cfg.get("mappings", []):
    canonical = m["canonical"]
    sub = df[df["source_file"]==m["source_file"]]
    if sub.empty: 
        print(f"[warn] mapping '{canonical}': source_file not found.")
        continue
    sub = sub[sub["subvariable"].astype(str).str.match(m["sheet_match"], na=False)]
    if sub.empty: 
        print(f"[warn] mapping '{canonical}': sheet_match found no rows.")
        continue
    sub = sub[sub["source_series_id"].astype(str).str.match(m["series_match"], na=False)]
    if sub.empty:
        print(f"[warn] mapping '{canonical}': series_match found no rows.")
        continue
    sub=sub.dropna(subset=["value"]).copy()
    if sub.empty:
        print(f"[warn] mapping '{canonical}' matched labels but no numeric values; skipping.")
        continue
    pf=sub["period"].astype(str).map(norm_period)
    sub["period"]=pf.map(lambda x:x[0]); sub["frequency"]=pf.map(lambda x:x[1])
    any_text=(sub["source_sheet"].astype(str)+" "+sub["source_series_id"].astype(str)).str.lower().iloc[0]
    sub["units"]=infer_units(canonical, any_text) or sub["units"]
    sub["variable"]=canonical
    rows.append(sub[["vintage","frequency","period","variable","value","units","source_file","source_sheet","source_series_id"]])

if not rows:
    raise SystemExit("No rows matched mapping.yml — refine patterns using config/mapping_seed.csv.")

out=pd.concat(rows, ignore_index=True)
OUT.mkdir(parents=True, exist_ok=True)
out.to_parquet(OUT/"baseline_efo_2025-03.parquet", index=False)
out.head(200).to_csv(OUT/"baseline_efo_2025-03_sample.csv", index=False)
print("WROTE:", OUT/"baseline_efo_2025-03.parquet", "rows:", len(out))
