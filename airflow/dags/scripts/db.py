import os
from sqlalchemy import create_engine

def pg_url() -> str:
    host = os.getenv("PGHOST", "postgres")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "airflow")
    user = os.getenv("PGUSER", "airflow")
    pw = os.getenv("PGPASSWORD", "airflow")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"

def engine():
    return create_engine(pg_url(), pool_pre_ping=True)
