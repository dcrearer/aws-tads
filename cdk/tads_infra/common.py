"""Shared constants and Lambda-asset helpers for the TADS CDK constructs."""

import hashlib
import os
from pathlib import Path

from aws_cdk import aws_lambda as lambda_

REPO_ROOT = Path(__file__).resolve().parents[2]
PLACEHOLDER = REPO_ROOT / "cdk" / "placeholder"

DB_NAME = "tads"
DB_USER = "tads"


def migrations_hash() -> str:
    """SHA-256 over the sorted migration files, so the migrate custom resource re-runs whenever
    the migration set changes."""
    migrations = sorted((REPO_ROOT / "crates" / "tads-store" / "migrations").glob("*.sql"))
    h = hashlib.sha256()
    for f in migrations:
        h.update(f.name.encode())
        h.update(f.read_bytes())
    return h.hexdigest()[:16]


def lambda_code(name: str) -> lambda_.Code:
    """Resolve a Lambda's code asset, falling back to a placeholder only when explicitly allowed."""
    real = REPO_ROOT / "target" / "lambda" / name
    if (real / "bootstrap").exists():
        return lambda_.Code.from_asset(str(real))
    if os.environ.get("CDK_PLACEHOLDER"):
        return lambda_.Code.from_asset(str(PLACEHOLDER))
    raise RuntimeError(
        f"Lambda artifact for '{name}' not found at {real}. Run "
        "`./scripts/build-lambdas.sh` first, or set CDK_PLACEHOLDER=1 to synth "
        "without the compiled binaries."
    )
