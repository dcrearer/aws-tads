"""HTTP API — API Gateway v2 fronted by the custom Lambda authorizer, with CORS + access logging."""

import json

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_apigatewayv2_authorizers as apigw_authz
from aws_cdk import aws_apigatewayv2_integrations as apigw_integ
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from constructs import Construct

from .compute import Compute


class Api(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        key: kms.Key,
        compute: Compute,
    ) -> None:
        super().__init__(scope, construct_id)
        stack = Stack.of(self)

        self.http_api = apigw.HttpApi(
            self,
            "HttpApi",
            api_name=f"{prefix}-api",
            create_default_stage=True,
            # Allow browser SPAs (e.g. a TADS Explorer UI) to call the API cross-origin. Every
            # route still requires a bearer token; CORS only governs which browser origins may
            # read responses. Preflight OPTIONS is answered by API Gateway (not the authorizer).
            cors_preflight=apigw.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[apigw.CorsHttpMethod.ANY],
                allow_headers=["authorization", "content-type"],
                max_age=Duration.days(1),
            ),
        )

        key.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[iam.ServicePrincipal(f"logs.{stack.region}.amazonaws.com")],
                actions=[
                    "kms:Encrypt*",
                    "kms:Decrypt*",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:Describe*",
                ],
                resources=["*"],
                conditions={
                    "ArnLike": {
                        "kms:EncryptionContext:aws:logs:arn": (
                            f"arn:aws:logs:{stack.region}:{stack.account}:log-group:*"
                        )
                    }
                },
            )
        )
        access_logs = logs.LogGroup(
            self,
            "ApiAccessLogs",
            log_group_name=f"/aws/apigateway/{prefix}-api/access",
            retention=logs.RetentionDays.ONE_MONTH,
            encryption_key=key,
            removal_policy=RemovalPolicy.DESTROY,  # dev
        )
        default_stage = self.http_api.default_stage.node.default_child
        default_stage.access_log_settings = apigw.CfnStage.AccessLogSettingsProperty(
            destination_arn=access_logs.log_group_arn,
            format=json.dumps(
                {
                    "requestId": "$context.requestId",
                    "ip": "$context.identity.sourceIp",
                    "requestTime": "$context.requestTime",
                    "httpMethod": "$context.httpMethod",
                    "routeKey": "$context.routeKey",
                    "path": "$context.path",
                    "status": "$context.status",
                    "protocol": "$context.protocol",
                    "responseLength": "$context.responseLength",
                    "integrationError": "$context.integrationErrorMessage",
                }
            ),
        )

        authorizer = apigw_authz.HttpLambdaAuthorizer(
            "LambdaAuthorizer",
            compute.fn_authorizer,
            authorizer_name=f"{prefix}-authorizer",
            response_types=[apigw_authz.HttpLambdaResponseType.SIMPLE],
            identity_source=["$request.header.Authorization"],
            results_cache_ttl=Duration.minutes(5),
        )

        # Explicit methods (not ANY) so OPTIONS is NOT routed to the authorizer — API Gateway then
        # answers the CORS preflight itself (204 + headers), which browsers require.
        methods = [
            apigw.HttpMethod.GET,
            apigw.HttpMethod.HEAD,
            apigw.HttpMethod.PUT,
            apigw.HttpMethod.POST,
            apigw.HttpMethod.DELETE,
        ]

        def route(path: str, fn: lambda_.Function, integ_id: str) -> None:
            self.http_api.add_routes(
                path=path,
                methods=methods,
                integration=apigw_integ.HttpLambdaIntegration(integ_id, fn),
                authorizer=authorizer,
            )

        route("/", compute.fn_service, "RootInteg")
        route("/service", compute.fn_service, "ServiceInteg")
        route("/service/webhooks", compute.fn_webhooks, "WebhooksInteg")
        route("/service/webhooks/{proxy+}", compute.fn_webhooks, "WebhooksProxyInteg")
        route("/schemas", compute.fn_schemas, "SchemasInteg")
        route("/schemas/{proxy+}", compute.fn_schemas, "SchemasProxyInteg")
        route("/tracks/{track_id}/events", compute.fn_events, "EventsInteg")
        route("/tracks", compute.fn_tracks, "TracksInteg")
        route("/tracks/{proxy+}", compute.fn_tracks, "TracksProxyInteg")
        route("/track-delete-requests", compute.fn_delete_requests, "DeleteReqInteg")
        route("/track-delete-requests/{proxy+}", compute.fn_delete_requests, "DeleteReqProxyInteg")
