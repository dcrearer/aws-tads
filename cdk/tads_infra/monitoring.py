"""Monitoring — CloudWatch alarms + an operational dashboard, wired to an SNS topic.

Closes the ops blind spot: a webhook silently landing in a DLQ, a spike of API `5xx`, or Aurora
approaching its connection ceiling now raise alarms instead of going unnoticed. Alarm/OK actions
publish to a CMK-encrypted SNS topic; pass `-c alarm_email=<addr>` to auto-subscribe an inbox
(otherwise the topic is created unsubscribed for you to wire endpoints to).
"""

from aws_cdk import CfnOutput, Duration
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from constructs import Construct

from .api import Api
from .compute import Compute
from .database import Database
from .messaging import Messaging


class Monitoring(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        key: kms.Key,
        messaging: Messaging,
        database: Database,
        compute: Compute,
        api: Api,
    ) -> None:
        super().__init__(scope, construct_id)

        # CMK-encrypted alarm topic. CloudWatch must be able to use the key to publish to it.
        key.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[iam.ServicePrincipal("cloudwatch.amazonaws.com")],
                actions=["kms:Decrypt", "kms:GenerateDataKey*"],
                resources=["*"],
            )
        )
        self.topic = sns.Topic(self, "AlarmTopic", topic_name=f"{prefix}-alarms", master_key=key)
        email = self.node.try_get_context("alarm_email")
        if email:
            self.topic.add_subscription(subs.EmailSubscription(email))
        action = cw_actions.SnsAction(self.topic)

        def make_alarm(cid: str, metric: cw.IMetric, **kw) -> cw.Alarm:
            a = cw.Alarm(
                self,
                cid,
                metric=metric,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
                **kw,
            )
            a.add_alarm_action(action)
            a.add_ok_action(action)
            return a

        # 1. DLQ depth — any message in a DLQ means delivery/processing exhausted retries.
        dlqs = {
            "DeleteDlq": messaging.delete_dlq,
            "LambdaDlq": messaging.lambda_dlq,
            "WebhookPokeDlq": messaging.webhook_poke_dlq,
            "WebhookDeliveryDlq": messaging.webhook_delivery_dlq,
        }
        self.dlq_metrics = []
        for name, q in dlqs.items():
            m = q.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(1), statistic="Maximum"
            )
            self.dlq_metrics.append(m)
            make_alarm(f"{name}DepthAlarm", m, threshold=1, evaluation_periods=1)

        # 2. API Gateway 5xx — sustained server errors (3 of 5 one-minute periods).
        self.api_5xx = api.http_api.metric_server_error(
            period=Duration.minutes(1), statistic="Sum"
        )
        make_alarm(
            "Api5xxAlarm", self.api_5xx, threshold=5, evaluation_periods=5, datapoints_to_alarm=3
        )

        # 3. Aurora connection pressure. The static threshold should track the instance's
        #    `max_connections` at the serverless_v2 floor; revisit if `db_lambda_concurrency` or the
        #    ACU range changes. (DB demand ceiling = db_lambda_concurrency x count of DB Lambdas.)
        self.db_conns = database.cluster.metric_database_connections(
            period=Duration.minutes(1), statistic="Maximum"
        )
        make_alarm(
            "AuroraConnectionsAlarm",
            self.db_conns,
            threshold=80,
            evaluation_periods=5,
            datapoints_to_alarm=3,
        )

        self._dashboard(prefix, compute, api, database)

        CfnOutput(self, "AlarmTopicArn", value=self.topic.topic_arn)

    def _dashboard(self, prefix: str, compute: Compute, api: Api, database: Database) -> None:
        fns = [
            compute.fn_service,
            compute.fn_schemas,
            compute.fn_tracks,
            compute.fn_events,
            compute.fn_webhooks,
            compute.fn_delete_requests,
            compute.fn_delete_worker,
            compute.fn_relay,
            compute.fn_delivery,
            compute.fn_webhook_dlq,
            compute.fn_authorizer,
        ]
        dashboard = cw.Dashboard(self, "Dashboard", dashboard_name=f"{prefix}-ops")
        dashboard.add_widgets(
            cw.GraphWidget(title="DLQ depth", left=self.dlq_metrics, width=12),
            cw.GraphWidget(
                title="API requests / errors",
                left=[api.http_api.metric_count(statistic="Sum")],
                right=[
                    api.http_api.metric_client_error(statistic="Sum"),
                    api.http_api.metric_server_error(statistic="Sum"),
                ],
                width=12,
            ),
            cw.GraphWidget(
                title="Aurora connections / CPU",
                left=[self.db_conns],
                right=[database.cluster.metric_cpu_utilization()],
                width=12,
            ),
            cw.GraphWidget(
                title="Lambda errors", left=[f.metric_errors() for f in fns], width=12
            ),
            cw.GraphWidget(
                title="Lambda throttles", left=[f.metric_throttles() for f in fns], width=12
            ),
            cw.GraphWidget(
                title="Lambda duration (p95)",
                left=[f.metric_duration(statistic="p95") for f in fns],
                width=12,
            ),
        )
