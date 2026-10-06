#!/usr/bin/env python3
"""CDK entry point for the TADS API infrastructure."""

import os

import aws_cdk as cdk

from tads_infra.stack import TadsStack

app = cdk.App()

# Resource-name prefix (per environment). Override with `-c prefix=...`.
prefix = app.node.try_get_context("prefix") or "tads-dev"

TadsStack(
    app,
    "TadsStack",
    prefix=prefix,
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
