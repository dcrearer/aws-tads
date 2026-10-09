"""Networking — isolated VPC (no NAT) + interface endpoints + the shared Lambda security group."""

from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class Network(Construct):
    """Isolated VPC (no NAT). DB-touching Lambdas, Aurora and RDS Proxy live in private subnets;
    interface endpoints give them Secrets Manager / SQS / KMS / STS without egress. The authorizer
    and the webhook delivery worker stay OUT of the VPC (they need public egress and touch no DB)."""

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
        )
        for name, svc in [
            ("SecretsManagerEndpoint", ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER),
            ("SqsEndpoint", ec2.InterfaceVpcEndpointAwsService.SQS),
            ("KmsEndpoint", ec2.InterfaceVpcEndpointAwsService.KMS),
            ("StsEndpoint", ec2.InterfaceVpcEndpointAwsService.STS),
        ]:
            self.vpc.add_interface_endpoint(name, service=svc)

        # One shared security group for all in-VPC Lambdas.
        self.lambda_sg = ec2.SecurityGroup(
            self,
            "LambdaSg",
            vpc=self.vpc,
            description="TADS in-VPC Lambdas",
            allow_all_outbound=True,
        )

    @property
    def isolated_subnets(self) -> ec2.SubnetSelection:
        return ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED)
