import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from sourceconnection import get_engine
BASE = Path(__file__).resolve().parent.parent
CONFIG = BASE / "config" / "tables.yaml"
SRC_DIR = BASE / "src"
LOG_DIR = BASE / "logs"

STG, TGT, AUDIT = "stg", "tgt", "audit"
VALID_LOAD_TYPES = ("full", "upsert")
VALID_DELETE_MODES = ("soft", "hard", "none")

logger = logging.getLogger("ingest")


# ---------------------------------------------------------------- helpers
def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / "ingest.log", encoding="utf-8"),
        ],
    )


def q(name):
    return f'"{name}"'


# ----------------------------------------------------------------- config
def validate_table_cfg(t):
    for key in ("name", "source_file"):
        if not t.get(key):
            raise ValueError(f"Table config is missing '{key}': {t}")

    if not re.fullmatch(r"[a-z_][a-z0-9_]*", t["name"]):
        raise ValueError(
            f"Invalid table name '{t['name']}' (use lowercase letters, digits, underscore)"
        )

    load_type = t.get("load_type", "upsert")
    if load_type not in VALID_LOAD_TYPES:
        raise ValueError(f"{t['name']}: load_type must be one of {VALID_LOAD_TYPES}")

    if load_type == "upsert":
        if not t.get("primary_key"):
            raise ValueError(f"{t['name']}: upsert needs 'primary_key' in config")
        if t.get("delete_mode", "soft") not in VALID_DELETE_MODES:
            raise ValueError(f"{t['name']}: delete_mode must be one of {VALID_DELETE_MODES}")


def load_config():
    try:
        with open(CONFIG, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError as e:
        raise ValueError(f"Config file not found: {CONFIG}") from e
    except yaml.YAMLError as e:
        raise ValueError(f"Config file is not valid YAML: {e}") from e

    tables = (cfg or {}).get("tables")
    if not tables:
        raise ValueError("No tables defined in config")

    seen = set()
    for t in tables:
        validate_table_cfg(t)
        if t["name"] in seen:
            raise ValueError(f"Duplicate table name in config: {t['name']}")
        seen.add(t["name"])
    return tables


# ----------------------------------------------------------------- source
def read_source(cfg):
    path = SRC_DIR / cfg["source_file"]
    try:
        df = pd.read_csv(path)
    except FileNotFoundError as e:
        raise ValueError(f"Source file not found: {path}") from e
    except pd.errors.EmptyDataError as e:
        raise ValueError(f"Source file is empty: {path.name}") from e
    except (pd.errors.ParserError, UnicodeDecodeError) as e:
        raise ValueError(f"Could not parse {path.name}: {e}") from e

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if df.columns.duplicated().any():
        raise ValueError(f"{path.name}: duplicate column names after cleaning")
    if df.empty:
        raise ValueError(f"{path.name} has no rows, skipping to avoid wiping target")
    return df


# --------------------------------------------------------------------- db
def ensure_infra(conn):
    for schema in (STG, TGT, AUDIT):
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT}.etl_run_log (
            run_id      SERIAL PRIMARY KEY,
            table_name  TEXT,
            started_at  TIMESTAMP,
            finished_at TIMESTAMP,
            status      TEXT,
            rows_read   INT,
            inserted    INT,
            updated     INT,
            deleted     INT,
            error       TEXT
        )
    """))


def ensure_target(conn, name, pk):
    t = f"{TGT}.{q(name)}"
    conn.execute(text(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM {STG}.{q(name)} WHERE FALSE"))
    conn.execute(text(f"""
        ALTER TABLE {t}
            ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP,
            ADD COLUMN IF NOT EXISTS loaded_at  TIMESTAMP DEFAULT now(),
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now()
    """))
    pk_list = ", ".join(q(c) for c in pk)
    conn.execute(text(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{name}_pk ON {t} ({pk_list})"))


def merge(conn, name, cols, pk):
    t, s = f"{TGT}.{q(name)}", f"{STG}.{q(name)}"
    non_pk = [c for c in cols if c not in pk]

    col_list = ", ".join(q(c) for c in cols)
    pk_list = ", ".join(q(c) for c in pk)
    set_cols = "".join(f"{q(c)} = EXCLUDED.{q(c)}, " for c in non_pk)
    old = ", ".join([f"t.{q(c)}" for c in non_pk] + ["t.is_deleted"])
    new = ", ".join([f"EXCLUDED.{q(c)}" for c in non_pk] + ["FALSE"])

    rows = conn.execute(text(f"""
        INSERT INTO {t} AS t ({col_list})
        SELECT {col_list} FROM {s} WHERE TRUE
        ON CONFLICT ({pk_list}) DO UPDATE SET
            {set_cols}is_deleted = FALSE, deleted_at = NULL, updated_at = now()
        WHERE ({old}) IS DISTINCT FROM ({new})
        RETURNING (xmax = 0) AS inserted
    """)).fetchall()

    inserted = sum(1 for r in rows if r[0])
    return inserted, len(rows) - inserted


def handle_deletes(conn, name, pk, mode):
    if mode == "none":
        return 0
    t, s = f"{TGT}.{q(name)}", f"{STG}.{q(name)}"
    join = " AND ".join(f"s.{q(c)} = t.{q(c)}" for c in pk)
    missing = f"NOT EXISTS (SELECT 1 FROM {s} s WHERE {join})"

    if mode == "soft":
        sql = f"""UPDATE {t} t SET is_deleted = TRUE, deleted_at = now(), updated_at = now()
                  WHERE t.is_deleted = FALSE AND {missing}"""
    else:  # hard
        sql = f"DELETE FROM {t} t WHERE {missing}"
    return conn.execute(text(sql)).rowcount


def full_load(conn, name, cols):
    t, s = f"{TGT}.{q(name)}", f"{STG}.{q(name)}"
    col_list = ", ".join(q(c) for c in cols)

    conn.execute(text(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM {s} WHERE FALSE"))
    conn.execute(text(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMP DEFAULT now()"))

    deleted = conn.execute(text(f"SELECT count(*) FROM {t}")).scalar()
    conn.execute(text(f"TRUNCATE {t}"))
    conn.execute(text(f"INSERT INTO {t} ({col_list}) SELECT {col_list} FROM {s}"))
    inserted = conn.execute(text(f"SELECT count(*) FROM {t}")).scalar()
    return inserted, deleted


# --------------------------------------------------------------- one table
def run_table(engine, cfg):
    name = cfg["name"]
    load_type = cfg.get("load_type", "upsert")

    df = read_source(cfg)

    pk = []
    if load_type == "upsert":
        pk = [c.lower() for c in cfg["primary_key"]]
        missing_cols = [c for c in pk if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Primary key column(s) not in CSV: {missing_cols}")
        if df.duplicated(subset=pk).any():
            raise ValueError(f"Duplicate primary keys in source file for {pk}")

    # 1) staging mein fresh copy
    df.to_sql(name, engine, schema=STG, if_exists="replace", index=False)

    # 2) target load: ek transaction, error aaye to automatic rollback
    with engine.begin() as conn:
        if load_type == "full":
            inserted, deleted = full_load(conn, name, list(df.columns))
            updated = 0
        else:
            ensure_target(conn, name, pk)
            inserted, updated = merge(conn, name, list(df.columns), pk)
            deleted = handle_deletes(conn, name, pk, cfg.get("delete_mode", "soft"))

    return dict(rows_read=len(df), inserted=inserted, updated=updated, deleted=deleted)


# ------------------------------------------------------------------ audit
def log_run(engine, name, started, status, stats, error=None):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {AUDIT}.etl_run_log
                    (table_name, started_at, finished_at, status,
                     rows_read, inserted, updated, deleted, error)
                VALUES (:n, :s, now(), :st, :r, :i, :u, :d, :e)
            """), dict(n=name, s=started, st=status, r=stats["rows_read"],
                       i=stats["inserted"], u=stats["updated"], d=stats["deleted"],
                       e=(error or "")[:1000] or None))
    except SQLAlchemyError:
        # audit fail hone se poora run nahi rukna chahiye
        logger.exception("Could not write audit log for %s", name)


# ------------------------------------------------------------------- main
def main():
    setup_logging()
    engine = None

    try:
        tables = load_config()
        engine = get_engine()
        with engine.begin() as conn:
            ensure_infra(conn)
    except ValueError as e:
        logger.error("Startup failed: %s", e)
        return 1
    except KeyError as e:
        logger.error("Missing environment variable %s (check your .env file)", e)
        return 1
    except SQLAlchemyError as e:
        logger.error("Database connection/setup failed: %s", getattr(e, "orig", e))
        return 1

    ok, failed = 0, 0
    try:
        for table in tables:
            name = table["name"]
            started = datetime.now()
            zero = dict(rows_read=0, inserted=0, updated=0, deleted=0)

            try:
                stats = run_table(engine, table)
            except Exception as e:  # ek table fail ho to baaki chalte rahein
                failed += 1
                msg = str(getattr(e, "orig", e))
                logger.exception("[FAIL] %s: %s", name, msg)
                log_run(engine, name, started, "FAILED", zero, msg)
            else:
                ok += 1
                logger.info("[OK]   %s (%s): %s", name, table.get("load_type", "upsert"), stats)
                log_run(engine, name, started, "SUCCESS", stats)
    finally:
        engine.dispose()

    logger.info("Run finished: %d succeeded, %d failed", ok, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())