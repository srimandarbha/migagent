#!/usr/bin/env python3
"""Bootstrap PostgreSQL SRE Tracker with Initial Operational Signatures and ActionPlans.

Run this script during environment provisioning or database migration to establish
PostgreSQL as the authoritative single source of truth for dynamic operational memory.
Zero runtime Python seed merge required once bootstrapped.
"""
import os
import sys
from pathlib import Path

# Ensure project root is in python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.memory.seed_data import get_seed_signatures
from persistence.repository import SRETrackerRepository, PostgresUnavailable


def bootstrap_knowledge(dsn: str) -> None:
    print(f"Connecting to PostgreSQL SRE Tracker at: {dsn}")
    try:
        repo = SRETrackerRepository(dsn)
    except PostgresUnavailable as exc:
        print(f"Error: {exc}")
        sys.exit(1)

    # Ensure schema is up to date
    schema_path = ROOT / "persistence" / "schema.sql"
    if schema_path.exists():
        print(f"Applying schema migrations from {schema_path}...")
        try:
            with repo.connection() as conn:
                conn.execute(schema_path.read_text(encoding="utf-8"))
                conn.commit()
            print("Schema migrations applied successfully.")
        except Exception as exc:
            print(f"Warning: Failed to execute schema.sql directly: {exc}")

    seeds = get_seed_signatures()
    print(f"Found {len(seeds)} operational failure signatures in seed catalog.")

    persisted_count = 0
    solution_count = 0

    for sig in seeds:
        sig_dict = sig.to_dict()
        repo.save_known_issue(sig_dict)
        persisted_count += 1
        solution_count += len(sig.solutions)
        print(f"  [+] Seeded: {sig.signature_id} ({len(sig.solutions)} solutions, domain={sig.domain})")

    # Verification read-back
    verified_issues = repo.get_known_issues()
    print("\n--- Bootstrap Verification ---")
    print(f"Total signatures in PostgreSQL: {len(verified_issues)} (Seeded: {persisted_count})")
    print(f"Total action plans persisted:   {solution_count}")
    print("PostgreSQL is now configured as the single authoritative operational knowledge store.")
    repo.close()


def main():
    host = os.getenv("PGHOST", "127.0.0.1")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "postgres")
    db = os.getenv("PGDATABASE", "migration_agent")
    port = os.getenv("PGPORT", "5432")
    default_dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    dsn = sys.argv[1] if len(sys.argv) > 1 else os.getenv("MFA_POSTGRES_DSN", default_dsn)
    bootstrap_knowledge(dsn)


if __name__ == "__main__":
    main()
