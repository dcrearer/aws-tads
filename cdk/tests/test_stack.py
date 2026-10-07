"""Synthesis assertions for the TADS infrastructure stack."""

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
    t.resource_count_is("AWS::DynamoDB::Table", 5)
    # 8 of the 9 workspace Lambdas are deployed (search-indexer is parked while OpenSearch is
    # deferred). Count the Rust app functions by runtime so CDK-managed custom-resource/provider
    # helper Lambdas (Python) don't skew the assertion.
    t.resource_properties_count_is(
        "AWS::Lambda::Function", {"Runtime": "provided.al2023"}, 8
    )
    t.resource_count_is("AWS::SQS::Queue", 3)  # delete queue + delete DLQ + shared Lambda DLQ
    t.resource_count_is("AWS::OpenSearchServerless::Collection", 0)
    t.resource_count_is("AWS::Cognito::UserPool", 1)
    t.resource_count_is("AWS::KMS::Key", 1)  # shared data-at-rest CMK


def test_data_stores_encrypted_with_cmk():
    t = _template()
    # DynamoDB tables reference a KMS key for SSE.
    t.has_resource_properties(
        "AWS::DynamoDB::Table",
        {"SSESpecification": {"SSEEnabled": True, "SSEType": "KMS"}},
    )
    # SQS queues are KMS-encrypted (KmsMasterKeyId present).
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


def test_tables_have_streams_enabled():
    t = _template()
    t.has_resource_properties(
        "AWS::DynamoDB::Table",
        {"StreamSpecification": {"StreamViewType": "NEW_AND_OLD_IMAGES"}},
    )


def test_delete_requests_table_has_ttl():
    t = _template()
    t.has_resource_properties(
        "AWS::DynamoDB::Table",
        {"TimeToLiveSpecification": {"AttributeName": "expiry", "Enabled": True}},
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


def test_default_user_created_with_secret():
    t = _template()
    # Credentials live in a Secrets Manager secret (not in the template/outputs).
    t.resource_count_is("AWS::SecretsManager::Secret", 1)
    # A custom resource provisions the Cognito user.
    t.resource_count_is("Custom::AWS", 0)  # not using AwsCustomResource
    t.has_output("DefaultUsername", {})
    t.has_output("DefaultUserSecretArn", {})
    t.has_output("GetTokenCommand", {})
