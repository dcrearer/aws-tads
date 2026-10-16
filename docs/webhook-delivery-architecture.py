"""Source for docs/webhook-delivery-architecture.png — the TADS webhook delivery pipeline.

Regenerate after editing:

    uv run --no-project --with diagrams python docs/webhook-delivery-architecture.py

Requires graphviz (`dot`) on PATH. Output is written next to this file.
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.database import Aurora
from diagrams.aws.integration import SQS
from diagrams.aws.network import APIGateway
from diagrams.onprem.client import Client, Users

TITLE = (
    "TADS webhook delivery: event-driven poke + SQS durable delivery + DLQ "
    "(no EventBridge schedule)"
)

graph_attr = {"fontsize": "20", "labelloc": "t", "pad": "0.5", "nodesep": "0.8", "ranksep": "1.2"}

with Diagram(
    TITLE,
    filename="docs/webhook-delivery-architecture",
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
):
    client = Users("API client")
    api = APIGateway("HTTP API")

    with Cluster("Write path (in VPC)"):
        mutation = Lambda("Mutation lambdas\n(tracks / events / schemas)")
        aurora = Aurora("Aurora PostgreSQL\n(via RDS Proxy)\ndata + outbox")

    with Cluster("Trigger"):
        poke = SQS("Poke queue\n(signal only)")
        poke_dlq = SQS("Poke DLQ")

    relay = Lambda("Relay lambda\n(drain outbox, fan-out)")

    with Cluster("Delivery"):
        delivery_q = SQS("Delivery queue\n(1 msg / change x subscriber)")
        delivery = Lambda("Delivery lambda")
        delivery_dlq = SQS("Delivery DLQ")
        dlq_handler = Lambda("DLQ handler\n(mark webhook error)")

    subscriber = Client("Webhook subscriber\n(external HTTPS)")

    client >> api >> mutation
    mutation >> Edge(label="data + outbox (one tx)") >> aurora
    mutation >> Edge(label="poke (after commit)") >> poke
    poke >> Edge(label="maxReceiveCount", style="dashed") >> poke_dlq
    poke >> Edge(label="event source") >> relay
    relay >> Edge(label="claim outbox (FOR UPDATE SKIP LOCKED)") >> aurora
    relay >> Edge(label="match + fan-out") >> delivery_q
    delivery_q >> Edge(label="event source") >> delivery
    delivery_q >> Edge(label="maxReceiveCount", style="dashed") >> delivery_dlq
    delivery_dlq >> dlq_handler
    dlq_handler >> Edge(label="status=error") >> aurora
    delivery >> Edge(label="POST callback (api_key header, retries)") >> subscriber
