"""Database — Aurora PostgreSQL Serverless v2 + RDS Proxy (TLS), CMK-encrypted, in the VPC."""

from aws_cdk import Duration, RemovalPolicy
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_kms as kms
from aws_cdk import aws_rds as rds
from constructs import Construct

from .common import DB_NAME, DB_USER
from .network import Network


class Database(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        network: Network,
        key: kms.Key,
    ) -> None:
        super().__init__(scope, construct_id)
        self.cluster = rds.DatabaseCluster(
            self,
            "Db",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                # 16.15 is offered in us-east-1 (16.4 is not). `.of()` avoids depending on a
                # specific CDK enum that may lag the available engine versions.
                version=rds.AuroraPostgresEngineVersion.of("16.15", "16")
            ),
            credentials=rds.Credentials.from_generated_secret(DB_USER, encryption_key=key),
            writer=rds.ClusterInstance.serverless_v2("writer"),
            serverless_v2_min_capacity=0.5,  # floor >0 avoids auto-pause cold starts
            serverless_v2_max_capacity=4,
            vpc=network.vpc,
            vpc_subnets=network.isolated_subnets,
            default_database_name=DB_NAME,
            storage_encryption_key=key,
            iam_authentication=True,  # available for future IAM-token auth; app uses the secret today
            monitoring_interval=Duration.seconds(60),  # enhanced monitoring
            removal_policy=RemovalPolicy.DESTROY,  # dev
        )
        self.secret = self.cluster.secret

        self.proxy = self.cluster.add_proxy(
            "Proxy",
            secrets=[self.secret],
            vpc=network.vpc,
            vpc_subnets=network.isolated_subnets,
            require_tls=True,
            db_proxy_name=f"{prefix}-proxy",
        )
        # Lambdas -> proxy -> cluster on 5432.
        self.proxy.connections.allow_from(network.lambda_sg, ec2.Port.tcp(5432))
        self.cluster.connections.allow_from(self.proxy, ec2.Port.tcp(5432))
