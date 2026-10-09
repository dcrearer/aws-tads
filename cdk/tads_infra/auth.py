"""Auth — Cognito user pool + resource server + hosted domain + M2M (client_credentials) client."""

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_cognito as cognito
from constructs import Construct


class Auth(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, prefix: str) -> None:
        super().__init__(scope, construct_id)
        stack = Stack.of(self)

        self.user_pool = cognito.UserPool(
            self,
            "UserPool",
            user_pool_name=f"{prefix}-users",
            self_sign_up_enabled=False,
            removal_policy=RemovalPolicy.DESTROY,  # dev
        )
        domain_prefix = self.node.try_get_context("cognito_domain_prefix") or f"{prefix}-{stack.account}"
        self.user_pool_domain = self.user_pool.add_domain(
            "Domain",
            cognito_domain=cognito.CognitoDomainOptions(domain_prefix=domain_prefix),
        )
        self.token_url = f"https://{domain_prefix}.auth.{stack.region}.amazoncognito.com/oauth2/token"

        scope_read = cognito.ResourceServerScope(scope_name="read", scope_description="Read access")
        scope_write = cognito.ResourceServerScope(scope_name="write", scope_description="Write access")
        scope_delete = cognito.ResourceServerScope(scope_name="delete", scope_description="Delete access")
        resource_server = self.user_pool.add_resource_server(
            "ResourceServer",
            identifier="tads-api",
            scopes=[scope_read, scope_write, scope_delete],
        )
        self.user_pool_client = self.user_pool.add_client(
            "ApiClient",
            user_pool_client_name=f"{prefix}-api-client",
            generate_secret=True,
            o_auth=cognito.OAuthSettings(
                flows=cognito.OAuthFlows(client_credentials=True),
                scopes=[
                    cognito.OAuthScope.resource_server(resource_server, scope_read),
                    cognito.OAuthScope.resource_server(resource_server, scope_write),
                    cognito.OAuthScope.resource_server(resource_server, scope_delete),
                ],
            ),
        )
        self.user_pool_client.node.add_dependency(resource_server)
