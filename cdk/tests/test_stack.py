"""Synthesis assertions for the TADS infrastructure stack."""

import os

import aws_cdk as cdk
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
    t.resource_count_is("AWS::Lambda::Function", 9)
    t.resource_count_is("AWS::SQS::Queue", 2)  # main + DLQ
    t.resource_count_is("AWS::OpenSearchServerless::Collection", 1)
    t.resource_count_is("AWS::Cognito::UserPool", 1)


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
