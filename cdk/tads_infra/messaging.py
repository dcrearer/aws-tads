"""Messaging — SQS: the delete queue (+DLQ), the webhook poke/delivery queues (+DLQs), and the
shared Lambda async-invocation DLQ. All CMK-encrypted."""

from aws_cdk import Duration
from aws_cdk import aws_kms as kms
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class Messaging(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, key: kms.Key) -> None:
        super().__init__(scope, construct_id)

        def queue(cid: str, name: str, **kw) -> sqs.Queue:
            return sqs.Queue(
                self,
                cid,
                queue_name=name,
                enforce_ssl=True,
                encryption=sqs.QueueEncryption.KMS,
                encryption_master_key=key,
                **kw,
            )

        self.delete_dlq = queue(
            "DeleteDLQ", f"{prefix}-delete-dlq", retention_period=Duration.days(14)
        )
        self.delete_queue = queue(
            "DeleteQueue",
            f"{prefix}-delete",
            visibility_timeout=Duration.seconds(180),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=self.delete_dlq),
        )
        self.lambda_dlq = queue(
            "LambdaDLQ", f"{prefix}-lambda-dlq", retention_period=Duration.days(14)
        )

        # -- Webhook delivery pipeline (replaces the EventBridge-scheduled dispatcher) --
        # Poke queue: a signal-only nudge sent by mutation handlers after commit; triggers the relay
        # (no EventBridge timer). Its DLQ captures pokes the relay repeatedly fails to process.
        self.webhook_poke_dlq = queue(
            "WebhookPokeDLQ", f"{prefix}-webhook-poke-dlq", retention_period=Duration.days(14)
        )
        self.webhook_poke_queue = queue(
            "WebhookPokeQueue",
            f"{prefix}-webhook-poke",
            visibility_timeout=Duration.seconds(120),  # >= relay timeout
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=self.webhook_poke_dlq),
        )
        # Delivery queue: one message per (change x subscriber), built by the relay; consumed by the
        # (out-of-VPC) delivery worker. Its DLQ triggers the dlq handler, which marks the webhook error.
        self.webhook_delivery_dlq = queue(
            "WebhookDeliveryDLQ",
            f"{prefix}-webhook-delivery-dlq",
            visibility_timeout=Duration.seconds(60),  # >= dlq-handler timeout
            retention_period=Duration.days(14),
        )
        self.webhook_delivery_queue = queue(
            "WebhookDeliveryQueue",
            f"{prefix}-webhook-delivery",
            visibility_timeout=Duration.seconds(60),  # >= delivery timeout
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=5, queue=self.webhook_delivery_dlq
            ),
        )
