#!/usr/bin/env python3
__test__ = False
import os

def main():
    import psycopg
    dsn=os.getenv('DATABASE_URL','postgresql://postgres:postgres@127.0.0.1:5432/migration_agent')
    with psycopg.connect(dsn) as conn:
        print('PostgreSQL:', conn.execute('select version()').fetchone()[0])
        print('pgvector:', conn.execute("select extversion from pg_extension where extname='vector'").fetchone())
        print('schemas:', [r[0] for r in conn.execute("select schema_name from information_schema.schemata where schema_name in ('sre','memory','knowledge','config') order by 1")])
        print('tables:', conn.execute("select count(*) from information_schema.tables where table_schema in ('sre','memory','knowledge','config')").fetchone()[0])

if __name__ == '__main__': main()
