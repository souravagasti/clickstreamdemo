from kafka import KafkaConsumer, TopicPartition
import json

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events-dlq"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    # auto_offset_reset="earliest",
    auto_offset_reset="latest",
    enable_auto_commit=False,
    group_id=f"{TOPIC}-reader",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

cust_id_map = dict()
partitions = []

print(f"Reading Kafka topic: {TOPIC}")
print("Press Ctrl+C to stop.\n")
#CUST-0012

try:
    for message in consumer:

        # print("KEY TYPE:", type(message.key))
        # print("KEY:", message.key)

        # print("VALUE TYPE:", type(message.value))
        # print("VALUE:", message.value)

        # print(
        #     f"partition={message.partition} "
        #     f"offset={message.offset}"
        # )
        op = (json.dumps(
            message.value,
            indent=2
        ))
        print(f"{message.partition}_{message.offset}")
        print(op)

        # cust_id_map[f"{message.partition}_{message.offset}"]= message.value["customer_id"]
        # cust_id=message.value.get("customer_id")
        # if str(cust_id).strip().upper() == "CUST-0012":
        #     # print("Found CUST-0012!")
        #     partitions.append(f"{message.partition}_{message.offset}")

        # print("-" * 80)

except KeyboardInterrupt:
    print("\nStopped.")
    # print(consumer)

finally:

    # print(partitions)


    # 1. Fetch all partition IDs assigned to this topic
    partition_ids = consumer.partitions_for_topic(TOPIC)

    if partition_ids:
        # 2. Build TopicPartition objects
        partitions = [TopicPartition(TOPIC, p) for p in partition_ids]
        
        # 3. Query the broker for beginning and end offsets
        start_offsets = consumer.beginning_offsets(partitions)
        end_offsets = consumer.end_offsets(partitions)
        
        for tp in partitions:
            start = start_offsets[tp]
            end = end_offsets[tp]
            total_messages = end - start  # Total messages currently stored on disk
            
            print(f"Partition {tp.partition}:")
            print(f"  -> Oldest Offset on Disk: {start}")
            print(f"  -> Next Appending Offset: {end}")
            print(f"  -> Total Messages Stored: {total_messages}")
    else:
        print(f"Topic {TOPIC} does not exist or has no metadata.")
    
    # partitions = {}
    # for k,v in cust_id_map:
    #     if v == "CUST-0012":
    #         print(k[:1])

    # print(cust_id_map)
    consumer.close()