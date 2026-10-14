"""Compute — all Rust Lambdas, their shared factory/env, SQS grants + event sources, and the
one-off migrate custom resource. This is where networking, DB, messaging and auth come together."""

import aws_cdk as cdk
from aws_cdk import Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import custom_resources as cr
from constructs import Construct

from .auth import Auth
from .common import DB_NAME, DB_USER, lambda_code, migrations_hash
from .database import Database
from .messaging import Messaging
from .network import Network


class Compute(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        key: kms.Key,
        network: Network,
        database: Database,
        messaging: Messaging,
        auth: Auth,
    ) -> None:
        super().__init__(scope, construct_id)
        self.prefix = prefix
        self.key = key
        self.network = network
        self.database = database
        self.messaging = messaging
        self.auth = auth

        self._build_functions()
        self._grant_permissions()
        self._wire_event_sources()
        self._build_migrate()

    def _common_env(self) -> dict:
        return {
            "TADS__DATABASE__PROXY_ENDPOINT": self.database.proxy.endpoint,
            "TADS__DATABASE__DB_NAME": DB_NAME,
            "TADS__DATABASE__USERNAME": DB_USER,
            "TADS__DATABASE__SECRET_ARN": self.database.secret.secret_arn,
            "TADS__DELETE_QUEUE_URL": self.messaging.delete_queue.queue_url,
            "TADS__POKE_QUEUE_URL": self.messaging.webhook_poke_queue.queue_url,
            "TADS__SERVICE__TYPE": "urn:x-tads:service:reference.rust",
            "TADS__SERVICE__API_VERSION": "0.1",
            "TADS__SERVICE__NAME": "TADS Reference (Rust)",
            "TADS__SERVICE__SERVICE_VERSION": "0.1.0",
            "RUST_LOG": "info,tads=debug",
        }

    def _make_fn(
        self,
        construct_id: str,
        bin_name: str,
        *,
        memory: int = 256,
        timeout: int = 30,
        reserved_concurrency: int = 20,
        in_vpc: bool = True,
        extra_env: dict | None = None,
    ) -> lambda_.Function:
        env = self._common_env()
        if extra_env:
            env.update(extra_env)
        layers = []
        adot_arn = self.node.try_get_context("adot_layer_arn")
        if adot_arn:
            layers.append(
                lambda_.LayerVersion.from_layer_version_arn(self, f"{construct_id}Adot", adot_arn)
            )
            env["AWS_LAMBDA_EXEC_WRAPPER"] = "/opt/otel-handler"

        fn = lambda_.Function(
            self,
            construct_id,
            function_name=f"{self.prefix}-{bin_name}",
            runtime=lambda_.Runtime.PROVIDED_AL2023,
            architecture=lambda_.Architecture.ARM_64,
            handler="bootstrap",
            code=lambda_code(bin_name),
            memory_size=memory,
            timeout=Duration.seconds(timeout),
            environment=env,
            environment_encryption=self.key,
            dead_letter_queue=self.messaging.lambda_dlq,
            reserved_concurrent_executions=reserved_concurrency,
            tracing=lambda_.Tracing.ACTIVE,
            layers=layers or None,
            vpc=self.network.vpc if in_vpc else None,
            vpc_subnets=self.network.isolated_subnets if in_vpc else None,
            security_groups=[self.network.lambda_sg] if in_vpc else None,
        )
        if in_vpc:
            # In-VPC functions connect to Aurora through the proxy using the generated secret.
            self.database.secret.grant_read(fn)
        return fn

    def _build_functions(self) -> None:
        self.fn_service = self._make_fn("ServiceFn", "service")
        self.fn_schemas = self._make_fn("SchemasFn", "schemas")
        self.fn_tracks = self._make_fn("TracksFn", "tracks")
        self.fn_events = self._make_fn("EventsFn", "events")
        self.fn_webhooks = self._make_fn("WebhooksFn", "webhooks")
        self.fn_delete_requests = self._make_fn("DeleteRequestsFn", "delete-requests")

        self.fn_delete_worker = self._make_fn("DeleteWorkerFn", "delete-worker", memory=512, timeout=120)

        # Webhook delivery pipeline (poke-driven; no EventBridge schedule):
        #  relay (in-VPC): poke-triggered, drains the outbox and fans out onto the delivery queue.
        self.fn_relay = self._make_fn(
            "WebhookRelayFn",
            "relay",
            memory=256,
            timeout=60,
            extra_env={"TADS__DELIVERY_QUEUE_URL": self.messaging.webhook_delivery_queue.queue_url},
        )
        #  delivery (OUT of VPC): POSTs the callback to public HTTPS subscribers (no DB, no NAT).
        self.fn_delivery = self._make_fn(
            "WebhookDeliveryFn", "delivery", memory=256, timeout=30, in_vpc=False
        )
        #  dlq (in-VPC): marks a webhook `error` after its deliveries exhaust retries.
        self.fn_webhook_dlq = self._make_fn("WebhookDlqFn", "dlq", memory=256, timeout=30)

        self._build_vu_pipeline()

        # Custom Lambda authorizer — OUT of the VPC (needs public egress to Cognito JWKS).
        stack = Stack.of(self)
        self.fn_authorizer = self._make_fn(
            "AuthorizerFn",
            "authorizer",
            memory=256,
            timeout=10,
            in_vpc=False,
            extra_env={
                "TADS_JWT_ISSUER": f"https://cognito-idp.{stack.region}.amazonaws.com/{self.auth.user_pool.user_pool_id}",
                "TADS_CLIENT_ID": self.auth.user_pool_client.user_pool_client_id,
            },
        )

    def _build_vu_pipeline(self) -> None:
        """Video-understanding pipeline: TAMS webhook receiver + Bedrock analysis worker."""
        # TAMS/TADS OAuth2 client-credentials are supplied inline via config/env (see below), not
        # Secrets Manager. The only VU secret is the inbound-webhook HMAC signing key.
        self.vu_hmac_secret = secretsmanager.Secret(
            self,
            "VuTamsHmacSecret",
            secret_name=f"{self.prefix}-vu-tams-hmac",
            encryption_key=self.key,
            description="HMAC-SHA256 signing key for inbound TAMS webhook signatures.",
        )

        ctx = self.node.try_get_context
        self.vu_model_id = ctx("vu_bedrock_model_id") or "us.twelvelabs.pegasus-1-5-v1:0"

        # Receiver (in-VPC): verify HMAC, enqueue to the analysis queue. SQS/Secrets reached via the
        # VPC interface endpoints; no public egress needed.
        self.fn_vu_webhook = self._make_fn(
            "VuWebhookFn",
            "vu-webhook",
            memory=256,
            timeout=30,
            extra_env={
                "VU__ANALYSIS_QUEUE_URL": self.messaging.analysis_queue.queue_url,
                "VU__TAMS__SIGNATURE_SECRET_ARN": self.vu_hmac_secret.secret_arn,
                "VU__TAMS__SIGNATURE_HEADER": "x-tams-signature",
            },
        )

        # Worker (OUT of VPC): needs public egress to TAMS/TADS/Bedrock/OAuth (the VPC has no NAT).
        self.fn_vu_worker = self._make_fn(
            "VuAnalysisWorkerFn",
            "vu-analysis-worker",
            memory=1024,  # ffmpeg assembly + base64 headroom
            timeout=300,
            reserved_concurrency=5,  # bound Bedrock inference concurrency
            in_vpc=False,
            extra_env={
                "VU__TAMS__BASE_URL": ctx("vu_tams_base_url") or "",
                "VU__TADS__BASE_URL": ctx("vu_tads_base_url") or "",
                "VU__BEDROCK__MODEL_ID": self.vu_model_id,
                "VU__BEDROCK__MAX_OUTPUT_TOKENS": "4096",
                "VU__WINDOW__LOOKBACK_N": str(ctx("vu_lookback_n") or 3),
                "VU__KEY_MOMENT__SCHEMA_EXTERNAL_ID": "vu:key-moment:v1",
            },
        )

        # TAMS + TADS control-plane OAuth are supplied INLINE (not Secrets Manager). Inject each
        # block only when its client secret is provided via context — an empty inline block would
        # otherwise make the client attempt OAuth with blank credentials. Absent => unauthenticated.
        for svc in ("tams", "tads"):
            secret = ctx(f"vu_{svc}_client_secret")
            if not secret:
                continue
            prefix = f"VU__{svc.upper()}__OAUTH__"
            for key, value in {
                f"{prefix}TOKEN_URL": ctx(f"vu_{svc}_token_url") or "",
                f"{prefix}CLIENT_ID": ctx(f"vu_{svc}_client_id") or "",
                f"{prefix}CLIENT_SECRET": secret,
                f"{prefix}SCOPE": ctx(f"vu_{svc}_scope") or "",
            }.items():
                self.fn_vu_worker.add_environment(key, value)

        # Optional operator-supplied ffmpeg layer for multi-segment clip assembly.
        ffmpeg_arn = ctx("ffmpeg_layer_arn")
        if ffmpeg_arn:
            self.fn_vu_worker.add_layers(
                lambda_.LayerVersion.from_layer_version_arn(self, "FfmpegLayer", ffmpeg_arn)
            )
            self.fn_vu_worker.add_environment("FFMPEG_PATH", "/opt/bin/ffmpeg")

    def _grant_permissions(self) -> None:
        # DB access is granted in _make_fn (secret read + SG path to the proxy). The only extra
        # grants are SQS send for the handlers that enqueue delete-request work.
        self.messaging.delete_queue.grant_send_messages(self.fn_tracks)
        self.messaging.delete_queue.grant_send_messages(self.fn_events)

        # Webhook poke: every handler that commits a mutation nudges the relay.
        for fn in (self.fn_schemas, self.fn_tracks, self.fn_events, self.fn_delete_worker):
            self.messaging.webhook_poke_queue.grant_send_messages(fn)
        # Relay fans out onto the delivery queue.
        self.messaging.webhook_delivery_queue.grant_send_messages(self.fn_relay)

        # Video-understanding: receiver enqueues analysis jobs + reads the HMAC key; worker invokes
        # the Pegasus model (cross-region inference profile + base model). TAMS/TADS OAuth creds are
        # inline config, so the worker needs no Secrets Manager grant.
        self.messaging.analysis_queue.grant_send_messages(self.fn_vu_webhook)
        self.vu_hmac_secret.grant_read(self.fn_vu_webhook)
        stack = Stack.of(self)
        self.fn_vu_worker.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=[
                    f"arn:aws:bedrock:{stack.region}:{stack.account}:inference-profile/{self.vu_model_id}",
                    "arn:aws:bedrock:*::foundation-model/twelvelabs.pegasus-1-5-v1:0",
                ],
            )
        )

    def _wire_event_sources(self) -> None:
        self.fn_delete_worker.add_event_source(
            sources.SqsEventSource(self.messaging.delete_queue, batch_size=10)
        )

        # Webhook pipeline — all event-driven (no schedule):
        #  poke -> relay (if the relay errors, the poke redelivers; its DLQ is the backstop).
        self.fn_relay.add_event_source(
            sources.SqsEventSource(self.messaging.webhook_poke_queue, batch_size=10)
        )
        #  delivery queue -> delivery worker (partial-batch failures so only failures retry -> DLQ).
        self.fn_delivery.add_event_source(
            sources.SqsEventSource(
                self.messaging.webhook_delivery_queue, batch_size=10, report_batch_item_failures=True
            )
        )
        #  delivery DLQ -> dlq handler (mark the webhook `error`).
        self.fn_webhook_dlq.add_event_source(
            sources.SqsEventSource(self.messaging.webhook_delivery_dlq, batch_size=10)
        )

        #  analysis queue -> analysis worker (partial-batch failures so only failures retry -> DLQ).
        self.fn_vu_worker.add_event_source(
            sources.SqsEventSource(
                self.messaging.analysis_queue, batch_size=10, report_batch_item_failures=True
            )
        )

    def _build_migrate(self) -> None:
        self.fn_migrate = self._make_fn("MigrateFn", "migrate", timeout=120, reserved_concurrency=1)
        provider = cr.Provider(self, "MigrateProvider", on_event_handler=self.fn_migrate)
        resource = cdk.CustomResource(
            self,
            "RunMigrations",
            service_token=provider.service_token,
            # Keyed to the migrations-dir hash so the resource re-fires whenever a migration is
            # added/changed (sqlx skips already-applied migrations, so re-running is safe).
            properties={"migrations_hash": migrations_hash()},
        )
        resource.node.add_dependency(self.database.proxy)
