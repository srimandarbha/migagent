#!/usr/bin/env python3
"""Create migration_agent if absent and apply the MFA schema."""
import os
import psycopg

HOST=os.getenv('PGHOST','127.0.0.1'); USER=os.getenv('PGUSER','postgres'); PASSWORD=os.getenv('PGPASSWORD','postgres'); DB=os.getenv('PGDATABASE','migration_agent'); PORT=os.getenv('PGPORT','5432')
admin=f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres'
target=f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}'
with psycopg.connect(admin, autocommit=True) as conn:
    exists=conn.execute('SELECT 1 FROM pg_database WHERE datname=%s',(DB,)).fetchone()
    if not exists:
        conn.execute(f'CREATE DATABASE "{DB}"')
        print(f'created database {DB}')
    else: print(f'database {DB} already exists')
from pathlib import Path
schema=Path(__file__).resolve().parents[1]/'sql'/'mfa_postgres.sql'
with psycopg.connect(target) as conn:
    conn.execute(schema.read_text(encoding='utf-8'))
    conn.execute("ALTER TABLE knowledge.documents ADD COLUMN IF NOT EXISTS content TEXT")
    conn.execute("ALTER TABLE knowledge.documents ALTER COLUMN content SET DEFAULT ''")
    conn.execute("UPDATE knowledge.documents SET content='' WHERE content IS NULL")
    conn.execute("ALTER TABLE knowledge.documents ALTER COLUMN content SET NOT NULL")
    conn.commit()
print('MFA schema applied')
