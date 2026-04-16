#!/usr/bin/env python3
"""Run pending SQL migrations against DATABASE_URL.

Usage: uv run python migrations/migrate.py
"""
import os
import re
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

MIGRATIONS_DIR = Path(__file__).parent


def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(database_url, sslmode="require", connect_timeout=10)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                applied_at TIMESTAMPTZ DEFAULT now()
            );
        """)
        conn.commit()

        cur.execute("SELECT version FROM schema_migrations")
        applied = {row[0] for row in cur.fetchall()}

    migrations = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = re.match(r"^(\d+)_(.+)\.sql$", path.name)
        if not match:
            continue
        version = int(match.group(1))
        name = match.group(2)
        migrations.append((version, name, path))

    applied_count = 0
    skipped_count = 0

    for version, name, path in migrations:
        if version in applied:
            skipped_count += 1
            continue

        sql = path.read_text()
        print(f"Applying {version:03d}_{name}...")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO schema_migrations (version, name) VALUES (%s, %s)",
                    (version, name),
                )
            conn.commit()
            applied_count += 1
        except Exception as e:
            conn.rollback()
            # Only print the exception TYPE publicly — psycopg2 errors can
            # include the full DATABASE_URL (with password) in their str.
            # For detail, re-run locally with LOG_LEVEL=DEBUG or inspect
            # the stack trace printed just below.
            print(f"ERROR applying {version:03d}_{name}: {type(e).__name__}", file=sys.stderr)
            # Short SQL error code if available (psycopg2) — safe to log.
            code = getattr(e, "pgcode", None)
            if code:
                print(f"  pgcode: {code}", file=sys.stderr)
            sys.exit(1)

    conn.close()
    print(f"Applied {applied_count} migrations, {skipped_count} already up-to-date")


if __name__ == "__main__":
    main()
