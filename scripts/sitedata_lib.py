"""Shared plumbing for the site exporters: one key-pair Snowflake connection,
q() rows-as-dicts, dump() into site/data. Kept out of export_site_data.py so
export_fleet.py can reuse it without importing (and thereby running) the
whole export."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "data"
# [rev 2026-09-03] tokyo added — its poller is live and it reaches every gold
# mart. Chicago stays out until CTA activates the beta key (docs/04). Note when
# reading tokyo's standings: its delay is operator-stated and rounded to whole
# minutes, so its OTP is not measured the same way as the other seven
# (docs/06 "Tokyo's punctuality number"). The site must say so wherever it ranks.
CITIES = ["nyc", "boston", "dc", "sf", "toronto", "helsinki", "zurich", "tokyo"]


def connect() -> snowflake.connector.SnowflakeConnection:
    key_path = Path(
        os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            Path.home() / ".snowflake/keys/transit_terraform_key.p8",
        )
    )
    pk = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    pkb = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "wqteqyy-ib47757"),
        user=os.environ.get("SNOWFLAKE_USER", "TERRAFORM_SVC"),
        private_key=pkb,
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSIT_PIPELINE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_XS"),
        database="TRANSIT",
        schema="GOLD",
    )


CUR = connect().cursor()


def q(sql: str) -> list[dict]:
    CUR.execute(sql)
    cols = [c[0].lower() for c in CUR.description]
    return [dict(zip(cols, r, strict=True)) for r in CUR.fetchall()]


def dump(name: str, obj) -> None:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, default=str, separators=(",", ":")))
    print(f"{name}: {path.stat().st_size:,} bytes", flush=True)


AS_OF = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
