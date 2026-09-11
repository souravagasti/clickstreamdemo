import asyncio
import json
import random
import uuid

from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError


# ============================================================
# Configuration
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9092"
EVENTS_TOPIC = "events"

NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1


# ============================================================
# Kafka Admin Functions
# ============================================================

def create_topic( #create_topic("events",3,1) delete_topic("events")
    topic=EVENTS_TOPIC,
    num_partitions=NUM_PARTITIONS,
    replication_factor=REPLICATION_FACTOR
):
    """
    Create a Kafka topic if it does not already exist.
    """

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="streaming-lab-admin"
    )

    try:
        admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
        ])

        print(f"Created topic: {topic}")

    except TopicAlreadyExistsError:
        print(f"Topic already exists: {topic}")

    finally:
        admin.close()


def delete_topic(topic=EVENTS_TOPIC):
    """
    Delete a Kafka topic.
    """

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="streaming-lab-admin"
    )

    try:
        admin.delete_topics([topic])
        print(f"Deleted topic: {topic}")

    except UnknownTopicOrPartitionError:
        print(f"Topic does not exist: {topic}")

    finally:
        admin.close()


def topic_exists(topic=EVENTS_TOPIC):
    """
    Check whether a Kafka topic exists.
    """

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="streaming-lab-admin"
    )

    try:
        topics = admin.list_topics()
        return topic in topics

    finally:
        admin.close()


# ============================================================
# Kafka Producer
# ============================================================

def create_producer():
    """
    Create a Kafka producer.
    """

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


# ============================================================
# Simulated Business Data
# ============================================================

customers = [
    f"CUST-{i:04d}"
    for i in range(1, 101)
]

products = [
    f"PROD-{i:04d}"
    for i in range(1, 51)
]

stores = [
    f"STORE-{i:02d}"
    for i in range(1, 11)
]


# Keep track of orders that actually exist.
#
# This is deliberately simple in-memory state.
# Later we can replace this with something more sophisticated
# when we start learning about stateful streaming.

orders = {}


# ============================================================
# Event Helpers
# ============================================================

def current_event_time():
    return datetime.now(timezone.utc).isoformat()


def create_base_event(event_type):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": current_event_time()
    }


# ============================================================
# Event Generators
# ============================================================

def generate_order():
    """
    Generate an ORDER_CREATED event.

    The order is stored in the local 'orders' dictionary so
    subsequent payment/shipment events can reference it.
    """

    order_id = f"ORD-{uuid.uuid4().hex[:8]}"

    customer_id = random.choice(customers)
    product_id = random.choice(products)

    quantity = random.randint(1, 5)
    amount = round(random.uniform(500, 5000), 2)

    event = create_base_event("ORDER_CREATED")

    event.update({
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "amount": amount,
        "currency": "INR",
        "store_id": random.choice(stores)
    })

    # Store information needed by future events.
    orders[order_id] = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "amount": amount
    }

    return event


def generate_payment():
    """
    Generate a PAYMENT_COMPLETED event for an existing order.

    Returns None if there are no orders yet.
    """

    if not orders:
        return None

    order = random.choice(list(orders.values()))

    event = create_base_event("PAYMENT_COMPLETED")

    event.update({
        "payment_id": f"PAY-{uuid.uuid4().hex[:8]}",
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "amount": order["amount"],
        "currency": "INR",
        "payment_method": random.choice([
            "CREDIT_CARD",
            "UPI",
            "DEBIT_CARD",
            "NET_BANKING"
        ]),
        "status": "SUCCESS"
    })

    return event


def generate_shipment():
    """
    Generate an ORDER_SHIPPED event for an existing order.

    Returns None if there are no orders yet.
    """

    if not orders:
        return None

    order = random.choice(list(orders.values()))

    event = create_base_event("ORDER_SHIPPED")

    event.update({
        "shipment_id": f"SHIP-{uuid.uuid4().hex[:8]}",
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "carrier": random.choice([
            "DHL",
            "FEDEX",
            "BLUEDART"
        ]),
        "warehouse_id": f"WH-{random.randint(1, 10):02d}",
        "shipping_method": random.choice([
            "STANDARD",
            "EXPRESS"
        ])
    })

    return event


def generate_product_view():
    """
    Generate a PRODUCT_VIEWED event.
    """

    event = create_base_event("PRODUCT_VIEWED")

    event.update({
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "session_id": f"SES-{uuid.uuid4().hex[:8]}",
        "device": random.choice([
            "MOBILE",
            "DESKTOP",
            "TABLET"
        ]),
        "page": "PRODUCT_DETAIL"
    })

    return event


# ============================================================
# Main Event Generator
# ============================================================

def generate_event():
    """
    Generate one event.

    ORDER_CREATED is given a higher probability because the
    other event types depend on orders existing.
    """

    event_type = random.choices(
        [
            "ORDER_CREATED",
            "PAYMENT_COMPLETED",
            "ORDER_SHIPPED",
            "PRODUCT_VIEWED"
        ],
        weights=[
            20,
            10,
            20,
            50
        ]
    )[0]

    if event_type == "ORDER_CREATED":
        return generate_order()

    if event_type == "PAYMENT_COMPLETED":
        return generate_payment()

    if event_type == "ORDER_SHIPPED":
        return generate_shipment()

    if event_type == "PRODUCT_VIEWED":
        return generate_product_view()


# ============================================================
# Asynchronous Event Producer
# ============================================================

async def generate_events(
    n=None,
    delay_ms=1000
):
    """
    Generate events asynchronously.

    Parameters
    ----------
    n : int or None
        Number of events to generate.

        n=100
            Generate 100 events.

        n=None
            Generate indefinitely.

    delay_ms : int
        Delay between events in milliseconds.

        delay_ms=1000
            One event every second.

        delay_ms=100
            One event every 100 ms.

        delay_ms=0
            Generate as fast as possible.
    """

    producer = create_producer()

    generated = 0

    try:

        while n is None or generated < n:

            event = generate_event()

            # Some event types cannot be generated yet if there
            # are no orders.
            if event is not None:

                producer.send(
                    EVENTS_TOPIC,
                    value=event,
                    key=event["customer_id"].encode("utf-8"),
                    # partition=1,
                )

                generated += 1

                print(
                    f"{generated:06d} | "
                    f"{event['event_type']:20} | "
                    f"{event['event_id']}"
                )

            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)

    except asyncio.CancelledError:

        print("\nEvent generator cancelled.")

    finally:

        producer.flush()
        producer.close()

        print(f"Generated {generated} events.")


# ============================================================
# Convenience Function
# ============================================================

def run_events(n=None, delay_ms=1000):
    """
    Synchronous wrapper around the async generator.

    Useful from a normal Python script.
    """

    asyncio.run(
        generate_events(
            n=n,
            delay_ms=delay_ms
        )
    )


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    # delete_topic("events")
    # delete_topic("orders")
    # delete_topic("shipments")
    # delete_topic("payments")
    # delete_topic("views")

    create_topic("events",3,1) 
    create_topic("orders",3,1) 
    create_topic("shipments",3,1) 
    create_topic("payments",3,1) 
    create_topic("views",3,1) 

    run_events(
        n=10000,
        delay_ms=100
    )