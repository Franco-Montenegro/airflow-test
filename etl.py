import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("etl")

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "flights_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
CSV_PATH = os.getenv("CSV_PATH", "./data/flights_sample.csv")
TABLE_NAME = os.getenv("TABLE_NAME", "flights")

def get_engine():
    url = URL.create("postgresql+psycopg2", username=DB_USER, password=DB_PASSWORD,
                     host=DB_HOST, port=DB_PORT, database=DB_NAME)
    return create_engine(url, pool_pre_ping=True)

def ensure_table(engine):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id BIGSERIAL PRIMARY KEY,
        flight_date DATE NOT NULL,
        origin VARCHAR(8) NOT NULL,
        destination VARCHAR(8) NOT NULL,
        airline VARCHAR(64) NOT NULL,
        price INTEGER NOT NULL CHECK (price >= 0),
        currency VARCHAR(8) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_date ON {TABLE_NAME}(flight_date);
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_orig_dest ON {TABLE_NAME}(origin, destination);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def load_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe el CSV en {path}")
    return pd.read_csv(path)

def basic_validate(df):
    req = ["flight_date","origin","destination","airline","price","currency"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas: {missing}")
    df = df.dropna(how="all")
    for c in ["origin","destination","airline","currency"]:
        df[c] = df[c].astype(str).str.strip().str.upper()
    df["flight_date"] = pd.to_datetime(df["flight_date"], errors="coerce").dt.date
    df = df.dropna(subset=["flight_date"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(-1).astype(int)
    df = df[df["price"] >= 0]
    df = df[df["origin"] != df["destination"]]
    return df

def add_metadata(df):
    df = df.copy()
    df["created_at"] = datetime.utcnow()
    return df

def load_to_db(df, engine):
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False, method="multi", chunksize=10000)

def main():
    try:
        log.info("Iniciando ETL")
        engine = get_engine()
        ensure_table(engine)
        log.info(f"Leyendo CSV desde {CSV_PATH}")
        df = load_csv(CSV_PATH)
        log.info(f"Filas leídas: {len(df)}")
        df = basic_validate(df)
        log.info(f"Filas tras validación: {len(df)}")
        df = add_metadata(df)
        log.info("Cargando a PostgreSQL…")
        load_to_db(df, engine)
        with engine.begin() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE DATE(created_at)=CURRENT_DATE")).scalar()
        log.info(f"Filas cargadas hoy: {result}")
        log.info("ETL OK")
    except Exception as e:
        log.exception(f"Fallo en ETL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
