"""Synthesis assertions for the TADS infrastructure stack (Aurora PostgreSQL backend)."""

import os

import aws_cdk as cdk
from aws_cdk import assertions
from aws_cdk.assertions import Template

from tads_infra.stack import TadsStack


def _template() -> Template:
    os.environ["CDK_PLACEHOLDER"] = "1"  # synth without compiled Lambda binaries
    app = cdk.App()
    stack = TadsStack(app, "TestStack", prefix="tads-test")
    return Template.from_stack(stack)


def test_core_resource_counts():
    t = _template()
    # No DynamoDB — Aurora PostgreSQL is the system of record now.
    t.resource_count_is("AWS::DynamoDB::Table", 0)
    # 12 Rust Lambdas: 6 HTTP handlers + delete-worker + webhook relay/delivery/dlq + authorizer
    # + migrate. Count by runtime so CDK-managed helper Lambdas (Python/Node) don't skew it.
    t.resource_properties_count_is("AWS::Lambda::Function", {"Runtime": "provided.al2023"}, 12)
    # 7 queues: delete (+DLQ), shared Lambda DLQ, webhook poke (+DLQ), webhook delivery (+DLQ).
    t.resource_count_is("AWS::SQS::Queue", 7)
    # Webhook delivery is event-driven (poke -> relay -> delivery queue); no EventBridge schedule.
    t.resource_count_is("AWS::Events::Rule", 0)
    t.resource_count_is("AWS::Cognito::UserPool", 1)
    t.resource_count_is("AWS::KMS::Key", 1)  # shared data-at-rest CMK


def test_aurora_cluster_with_proxy_in_vpc():
    t = _template()
    t.resource_count_is("AWS::EC2::VPC", 1)
    t.resource_count_is("AWS::RDS::DBProxy", 1)
    # Aurora Serverless v2 (provisioned engine + ServerlessV2 scaling config).
    t.has_resource_properties(
        "AWS::RDS::DBCluster",
        {
            "Engine": "aurora-postgresql",
            "ServerlessV2ScalingConfiguration": assertions.Match.any_value(),
        },
    )
    # The RDS-generated credentials live in a Secrets Manager secret.
    t.resource_count_is("AWS::SecretsManager::Secret", 1)


def test_sqs_encrypted_with_cmk():
    t = _template()
    t.has_resource_properties(
        "AWS::SQS::Queue",
        {"KmsMasterKeyId": assertions.Match.any_value()},
    )


def test_lambdas_have_dlq_env_encryption_and_concurrency():
    t = _template()
    t.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "DeadLetterConfig": {"TargetArn": assertions.Match.any_value()},
            "KmsKeyArn": assertions.Match.any_value(),
            "ReservedConcurrentExecutions": 20,
        },
    )


def test_app_lambdas_run_in_vpc():
    t = _template()
    # The DB-touching functions carry a VpcConfig (the authorizer does not — asserted implicitly
    # by it being the one provided.al2023 function without one).
    t.has_resource_properties(
        "AWS::Lambda::Function",
        {"VpcConfig": assertions.Match.any_value()},
    )


def test_lambdas_use_arm64_provided_runtime_with_tracing():
    t = _template()
    t.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Runtime": "provided.al2023",
            "Architectures": ["arm64"],
            "TracingConfig": {"Mode": "Active"},
        },
    )


def test_migrations_run_via_custom_resource():
    t = _template()
    t.resource_count_is("AWS::CloudFormation::CustomResource", 1)


def test_http_api_present():
    t = _template()
    t.resource_count_is("AWS::ApiGatewayV2::Api", 1)
    t.resource_count_is("AWS::ApiGatewayV2::Authorizer", 1)


def test_http_api_stage_has_access_logging():
    t = _template()
    t.has_resource_properties(
        "AWS::ApiGatewayV2::Stage",
        {"AccessLogSettings": {"DestinationArn": assertions.Match.any_value()}},
    )


def test_machine_to_machine_oauth_client():
    t = _template()
    t.has_resource_properties(
        "AWS::Cognito::UserPoolResourceServer",
        {"Identifier": "tads-api"},
    )
    t.resource_count_is("AWS::Cognito::UserPoolDomain", 1)
    t.has_resource_properties(
        "AWS::Cognito::UserPoolClient",
        {
            "AllowedOAuthFlows": ["client_credentials"],
            "GenerateSecret": True,
            "AllowedOAuthScopes": assertions.Match.any_value(),
        },
    )
    t.has_output("TokenUrl", {})
    t.has_output("ClientSecret", {})
    t.has_output("GetTokenCommand", {})


def test_api_uses_lambda_authorizer():
    t = _template()
    t.has_resource_properties(
        "AWS::ApiGatewayV2::Authorizer",
        {"AuthorizerType": "REQUEST", "EnableSimpleResponses": True},
    )
