from spark_init import get_spark_session
from dotenv import load_dotenv
import os,time
from pyspark.sql.functions import (
    col,
    get_json_object,
    current_timestamp,
    to_timestamp,
    expr,
    to_json,
    struct
)

load_dotenv()

KAFKA_BOOTSTRAP_SERVER=os.getenv("KAFKA_BOOTSTRAP_SERVER")
usecase = "inspectstreams" # watermarking, foreachbatch, deltawrite, inspectstreams
# print(KAFKA_BOOTSTRAP_SERVER)
spark = get_spark_session()

# spark.sparkContext.setLogLevel("ERROR") 

# 2. Connect to Kafka Stream
events = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .option("maxOffsetsPerTrigger", 5)\
    .load()
     # .option("startingOffsets", "earliest") \

events = events.withColumns({
    "event_type": get_json_object(
        col("value").cast("string"),
        "$.event_type"
    ),
    "customer_id": get_json_object(
        col("value").cast("string"),
        "$.customer_id"
    ),
    "product_id": get_json_object(
        col("value").cast("string"),
        "$.product_id"
    ),
    "event_time": to_timestamp(
        get_json_object(
            col("value").cast("string"),
            "$.event_time"
        )
    ),
    "processing_time": current_timestamp()
})

# 3. Print schema mapping layout to verify it connects 
# events.printSchema()

orders = events.filter(
    col("event_type") == "ORDER_CREATED"
)
try:
    query1 = (orders.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "orders") \
        .option("checkpointLocation", "/tmp/orders-checkpoint1") \
        .trigger(processingTime="5 seconds")\
        .start()
    )

    print("orders stream created")

    query1.lastProgress

except Exception as e:
    print(f"failed to write topic orders with error : {e}")


payments = events.filter(
    col("event_type") == "PAYMENT_COMPLETED"
)

query2 = (payments.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "payments") \
    .option("checkpointLocation", "/tmp/payments-checkpoint1") \
    .start()
)

print("payments stream created")

shipments = events.filter(
    col("event_type") == "ORDER_SHIPPED"
)

query3 = (shipments.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "shipments") \
    .option("checkpointLocation", "/tmp/shipments-checkpoint1") \
    .start()
)

print("shipments stream created")

product_views = events.filter(
    col("event_type") == "PRODUCT_VIEWED"
)

query4 = (product_views.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "views") \
    .option("checkpointLocation", "/tmp/views-checkpoint1") \
    .start()
)

print("views stream created")

product_views = product_views.select(
    "customer_id",
    "product_id",
    "event_time"
).withWatermark(
    "event_time",
    "5 seconds"
).withColumn("event_time", col("event_time").cast("timestamp"))

orders = orders.select(
    "customer_id",
    "product_id",
    "event_time"
).withWatermark(
    "event_time",
    "5 seconds"
).withColumn("event_time", col("event_time").cast("timestamp"))


# print(product_views.schema)
# print(orders.schema)

conversions  = product_views.alias("v").join(
    orders.alias("o"),
    expr("""
        v.customer_id = o.customer_id
        AND v.product_id = o.product_id
        AND o.event_time >= v.event_time
        AND o.event_time <= v.event_time + interval 30 seconds
    """)
).select(
    "o.customer_id",
    "o.product_id",
    "o.event_time"
)

conversions = conversions.select(
    to_json(
        struct(
            "customer_id",
            "product_id",
            "event_time"
        )
    ).alias("value"),
    to_json(
        struct(
            "customer_id"
        )
    ).alias("key"),
)

# print(conversions.schema)
# time.sleep(100)

query5 = (conversions.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "conversions") \
    .option("checkpointLocation", "/tmp/conversions-checkpoint1") \
    .start()
)

while True:
    time.sleep(5)

    print("\n========== ORDERS ==========")

    if query1.lastProgress:
        p = query1.lastProgress

        print("Batch ID:", p["batchId"])
        print("Input rows:", p["numInputRows"])
        print("Input rate:", p["inputRowsPerSecond"])
        print("Processed rate:", p["processedRowsPerSecond"])
        print("Duration:", p["durationMs"])
        # print(orders.head())
        # print(product_views.head())

    else:
        print(f"No completed orders batch yet")

    print("\n========== PAYMENTS ==========")

    if query2.lastProgress:
        p = query2.lastProgress

        print("Batch ID:", p["batchId"])
        print("Input rows:", p["numInputRows"])
        print("Input rate:", p["inputRowsPerSecond"])
        print("Processed rate:", p["processedRowsPerSecond"])
        print("Duration:", p["durationMs"])
    else:
        print(f"No completed payments batch yet")

    print("\n========== SHIPMENTS ==========")

    if query3.lastProgress:
        p = query3.lastProgress

        print("Batch ID:", p["batchId"])
        print("Input rows:", p["numInputRows"])
        print("Input rate:", p["inputRowsPerSecond"])
        print("Processed rate:", p["processedRowsPerSecond"])
        print("Duration:", p["durationMs"])
    else:
        print(f"No completed shipments batch yet")   

    print("\n========== VIEWS ==========")

    if query4.lastProgress:
        p = query4.lastProgress

        print("Batch ID:", p["batchId"])
        print("Input rows:", p["numInputRows"])
        print("Input rate:", p["inputRowsPerSecond"])
        print("Processed rate:", p["processedRowsPerSecond"])
        print("Duration:", p["durationMs"])
    else:
        print(f"No completed views batch yet")   


    print("\n========== CONVERSIONS ==========")

    if query5.lastProgress:
        p = query5.lastProgress

        print("Batch ID:", p["batchId"])
        print("Input rows:", p["numInputRows"])
        print("Input rate:", p["inputRowsPerSecond"])
        print("Processed rate:", p["processedRowsPerSecond"])
        print("Duration:", p["durationMs"])
    else:
        print(f"No completed conversions batch yet")                  

# spark.streams.awaitAnyTermination(timeout=300)