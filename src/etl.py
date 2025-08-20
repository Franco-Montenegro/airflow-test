# src/etl.py
import json
from pathlib import Path
import pandas as pd

BASE = Path("/opt/airflow/repo")
RAW = BASE / "data" / "raw" / "flights.csv"
PROCESSED = BASE / "data" / "processed"
ART = BASE / ".artifacts"
PROCESSED.mkdir(parents=True, exist_ok=True)
ART.mkdir(parents=True, exist_ok=True)

def main():
    if not RAW.exists():
        raise FileNotFoundError(f"No existe el CSV de entrada: {RAW}")

    df = pd.read_csv(RAW)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.dropna(how="all")

    # Intento parquet; si falla, guardo CSV
    out_path = PROCESSED / "flights.parquet"
    try:
        df.to_parquet(out_path, index=False)
        output = str(out_path)
    except Exception:
        out_path = PROCESSED / "flights.csv"
        df.to_csv(out_path, index=False)
        output = str(out_path)

    result = {"rows_loaded": int(len(df)), "output": output}
    (ART / "etl_result.json").write_text(json.dumps(result), encoding="utf-8")
    print(json.dumps(result))

if __name__ == "__main__":
    main()
