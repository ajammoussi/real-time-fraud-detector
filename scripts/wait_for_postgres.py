"""Wait for Postgres to accept connections.

Usage: python scripts/wait_for_postgres.py --timeout 60
"""
import time
import argparse
import os

import psycopg2


def wait(timeout: int = 60, interval: float = 1.0) -> bool:
    cfg = {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", 5432)),
        "dbname": os.environ.get("POSTGRES_DB", "mlops"),
        "user": os.environ.get("POSTGRES_USER", "mlops"),
        "password": os.environ.get("POSTGRES_PASSWORD", "mlops_secret"),
    }
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(
                host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"], connect_timeout=3
            )
            conn.close()
            print("postgres available")
            return True
        except Exception:
            time.sleep(interval)
    return False


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--timeout", type=int, default=60)
    args = p.parse_args()
    ok = wait(timeout=args.timeout)
    if not ok:
        raise SystemExit(1)