"""TADS API infrastructure stack (Aurora PostgreSQL backend).

A thin orchestrator that composes one construct per concern (see `tads_infra/*.py`):

- `Keys`       — a customer-managed CMK encrypting all data at rest.
- `Network`    — isolated VPC (NO NAT) with interface endpoints (Secrets Manager, SQS, KMS, STS).
- `Database`   — Aurora PostgreSQL Serverless v2 + RDS Proxy (TLS), CMK-encrypted.
- `Messaging`  — SQS: delete queue (+DLQ), webhook poke/delivery queues (+DLQs), shared Lambda DLQ.
- `Auth`       — Cognito user pool + resource server + hosted domain + M2M client.
- `Compute`    — the Rust Lambdas, SQS grants/event sources, and the migrate custom resource.
- `Api`        — the HTTP API Gateway behind the custom Lambda authorizer, with access logging.

The change feed is a transactional **outbox**: mutation handlers nudge a poke queue after commit;
the relay drains the outbox and fans each change out to a delivery queue (one message per
subscriber) with a DLQ — event-driven, no EventBridge schedule, no DynamoDB Streams. Search is
served natively by Postgres (no OpenSearch).

Lambda code is read from `target/lambda/<bin>/bootstrap` (build with `scripts/build-lambdas.sh`).
Set `CDK_PLACEHOLDER=1` to synth without the binaries.
"""

import aws_cdk as cdk
from aws_cdk import Stack
from constructs import Construct

from .api import Api
from .auth import Auth
from .compute import Compute
from .database import Database
from .keys import Keys
from .messaging import Messaging
from .network import Network


class TadsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.prefix = prefix

        self.keys = Keys(self, "Keys", prefix=prefix)
        self.network = Network(self, "Network")
        self.database = Database(self, "Database", prefix=prefix, network=self.network, key=self.keys.key)
        self.messaging = Messaging(self, "Messaging", prefix=prefix, key=self.keys.key)
        self.auth = Auth(self, "Auth", prefix=prefix)
        self.compute = Compute(
            self,
            "Compute",
            prefix=prefix,
            key=self.keys.key,
            network=self.network,
            database=self.database,
            messaging=self.messaging,
            auth=self.auth,
        )
        self.api = Api(self, "Api", prefix=prefix, key=self.keys.key, compute=self.compute)

        self._outputs()

    def _outputs(self) -> None:
        client = self.auth.user_pool_client
        client_id = client.user_pool_client_id
        token_url = self.auth.token_url
        cdk.CfnOutput(self, "ApiEndpoint", value=self.api.http_api.api_endpoint)
        cdk.CfnOutput(self, "UserPoolId", value=self.auth.user_pool.user_pool_id)
        cdk.CfnOutput(self, "UserPoolClientId", value=client_id)
        cdk.CfnOutput(self, "DbProxyEndpoint", value=self.database.proxy.endpoint)
        cdk.CfnOutput(
            self,
            "JwtIssuer",
            value=f"https://cognito-idp.{self.region}.amazonaws.com/{self.auth.user_pool.user_pool_id}",
        )
        cdk.CfnOutput(self, "TokenUrl", value=token_url)
        cdk.CfnOutput(self, "OAuthScopes", value="tads-api/read tads-api/write tads-api/delete")
        cdk.CfnOutput(
            self,
            "ClientSecret",
            value=client.user_pool_client_secret.unsafe_unwrap(),
            description="OAuth2 client secret (paste into Insomnia as client_secret)",
        )
        cdk.CfnOutput(
            self,
            "GetTokenCommand",
            value=(
                f"curl -s -X POST {token_url} "
                f"-H 'Content-Type: application/x-www-form-urlencoded' "
                f"-u {client_id}:{client.user_pool_client_secret.unsafe_unwrap()} "
                f"--data-urlencode grant_type=client_credentials "
                f"--data-urlencode 'scope=tads-api/read tads-api/write tads-api/delete' "
                f"| python3 -c 'import json,sys;print(json.load(sys.stdin)[\"access_token\"])'"
            ),
        )
