"""Install DAGSTER_SVC's Snowflake private key from SSM before dagster starts.

Run by the container entrypoint (webserver and daemon) when
SNOWFLAKE_KEY_SSM_PARAM is set. The parameter is a SecureString set out-of-band
by the operator; terraform only ever writes "PLACEHOLDER" (see
infra/modules/services), which is skipped so a half-provisioned box degrades to
"key missing" instead of a garbage PEM. Never prints key material.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def main() -> int:
    param = os.environ.get("SNOWFLAKE_KEY_SSM_PARAM")
    dest = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not param or not dest:
        return 0
    import boto3

    value = boto3.client("ssm").get_parameter(Name=param, WithDecryption=True)["Parameter"]["Value"]
    if value.strip() == "PLACEHOLDER":
        print(f"ssm {param} still PLACEHOLDER; skipping key install", file=sys.stderr)
        return 0
    path = Path(dest)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(mode=0o600, exist_ok=True)
    path.chmod(0o600)  # tighten before writing in case the file pre-existed
    path.write_text(value)
    print(f"snowflake key installed at {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
