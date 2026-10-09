"""KMS — one customer-managed CMK encrypting all data at rest."""

from aws_cdk import RemovalPolicy
from aws_cdk import aws_kms as kms
from constructs import Construct


class Keys(Construct):
    """A single customer-managed CMK (Aurora, SQS, Lambda env, logs all share it)."""

    def __init__(self, scope: Construct, construct_id: str, *, prefix: str) -> None:
        super().__init__(scope, construct_id)
        self.key = kms.Key(
            self,
            "DataKey",
            alias=f"alias/{prefix}",
            description=f"{prefix} data-at-rest CMK",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY,  # dev; use RETAIN in production
        )
