import os
from pathlib import Path
import psycopg

ROOT=Path(__file__).resolve().parents[1]
SCHEMA=ROOT/'sql'/'mfa_postgres.sql'

if __name__ == '__main__':
    dsn=os.environ.get('DATABASE_URL','postgresql://postgres:postgres@127.0.0.1:5432/migration_agent')
    with psycopg.connect(dsn) as conn:
        conn.execute(SCHEMA.read_text(encoding='utf-8'))
        conn.commit()
    print('Migration Failure Agent PostgreSQL schema applied.')
