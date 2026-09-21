import asyncio
import json
import random
import uuid
import os

from datetime import datetime, timezone, timedelta, date

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from dotenv import load_dotenv

load_dotenv()

createPoisonRecord=os.getenv("CREATEPOISONRECORD",False)
print(f"createPoisonRecord:{createPoisonRecord}")
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

def json_serializer(obj):
    # Check if the object is a datetime or date instance
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()  # Converts to "2026-09-14T23:41:00"
    raise TypeError(f"Type {type(obj)} not serializable")

def create_producer():
    """
    Create a Kafka producer.
    """

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: json.dumps(k).encode('utf-8'), 
        value_serializer=lambda v: json.dumps(v).encode("utf-8")

        # value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
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



# ============================================================
# Event Generators
# ============================================================

def create_base_event(event_type, event_time=None):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": (
            event_time or current_event_time()
        )
    }

def generate_journey():

    customer_id = random.choice(customers)
    product_id = random.choice(products)

    # print("customer_id,product_id: ",customer_id,product_id)

    base_time = datetime.now(timezone.utc)#.strftime("%Y-%m-%d %H:%M:%S")

    events = []

    # ========================================================
    # 1. PRODUCT VIEWED
    # ========================================================

    view = create_base_event(
        "PRODUCT_VIEWED",
        base_time
    )

    view.update({
        "customer_id": customer_id,
        "product_id": product_id,
        "session_id": f"SES-{uuid.uuid4().hex[:8]}",
        "device": random.choice([
            "MOBILE",
            "DESKTOP",
            "TABLET"
        ]),
        "page": "PRODUCT_DETAIL",
        "event_time":base_time.isoformat()
    })

    events.append(view)

    # ========================================================
    # 2. OPTIONAL CONVERSION
    # ========================================================

    converts = random.random() < 0.5

    if not converts:
        return events

    # --------------------------------------------------------
    # ORDER
    # --------------------------------------------------------

    delay_seconds = random.choice([
        5,
        10,
        20,
        25,
        35,
        60
    ])

    order_time = (
        base_time +
        timedelta(seconds=delay_seconds)
    )

    order_id = f"ORD-{uuid.uuid4().hex[:8]}"

    order = create_base_event(
        "ORDER_CREATED",
        order_time
    )

    amount = round(random.uniform(500, 5000), 2)

    order.update({
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "amount": amount,
        "currency": "INR",
        "store_id": random.choice(stores),
        "event_time":order_time.isoformat()
    })

    events.append(order)

    # --------------------------------------------------------
    # PAYMENT
    # --------------------------------------------------------

    payment_time = order_time + timedelta(seconds=random.randint(1, 10))

    payment = create_base_event(
        "PAYMENT_COMPLETED",
        payment_time
    )

    payment.update({
        "payment_id": f"PAY-{uuid.uuid4().hex[:8]}",
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "amount": amount,
        "currency": "INR",
        "payment_method": random.choice([
            "CREDIT_CARD",
            "UPI",
            "DEBIT_CARD",
            "NET_BANKING"
        ]),
        "event_time":payment_time.isoformat(),
        "status": "SUCCESS"
    })

    events.append(payment)

    # --------------------------------------------------------
    # SHIPMENT
    # --------------------------------------------------------

    shipment_time = payment_time + timedelta(seconds=random.randint(1, 10))

    shipment = create_base_event(
        "ORDER_SHIPPED",
        shipment_time
    )

    shipment.update({
        "shipment_id": f"SHIP-{uuid.uuid4().hex[:8]}",
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "carrier": random.choice([
            "DHL",
            "FEDEX",
            "BLUEDART"
        ]),
        "warehouse_id": f"WH-{random.randint(1, 10):02d}",
        "shipping_method": random.choice([
            "STANDARD",
            "EXPRESS"
        ]),
        "event_time":payment_time.isoformat()

    })

    events.append(shipment)

    if createPoisonRecord:
        
        poison = create_base_event(
        "POISON_RECORD_CREATED",
        base_time
        )

        poison.update({
        "customer_id": "dummy",
        "event_time":base_time.isoformat()
        })

        print("poison record created")

        events.append(poison)

    return events

async def generate_events(n=None, delay_ms=1000):

    producer = create_producer()

    generated = 0

    try:

        while n is None or generated < n:

            journey = generate_journey()

            for event in journey:

                producer.send(
                    EVENTS_TOPIC,
                    value=event,
                    key=event["customer_id"]
                )

                generated += 1

                print(
                    f"{generated:06d} | "
                    f"{event['event_type']:20} | "
                    f"{event['customer_id']} | "
                    f"{event.get('product_id', '')} | "
                    f"{event['event_time']}"
                )

            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)

    except asyncio.CancelledError:

        print("\nEvent generator cancelled.")

    finally:

        producer.flush()
        producer.close()

        print(f"Generated {generated} events.")

def run_events(n=None, delay_ms=1000):

    asyncio.run(
        generate_events(
            n=n,
            delay_ms=delay_ms
        )
    )   

if __name__ == "__main__":
    ...

    create_topic("events", 3, 1)
    create_topic("events", 3, 1)
    create_topic("views", 3, 1)
    create_topic("orders", 3, 1)
    create_topic("payments", 3, 1)
    create_topic("shipments", 3, 1)
    create_topic("conversions", 3, 1)
    create_topic("events_dlq", 3, 1)


    run_events(
        n=20,
        delay_ms=1000
    ) 