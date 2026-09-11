                        RAW EVENT STREAM
                              │
                    ┌─────────▼─────────┐
                    │ Kafka: events     │
                    │                   │
                    │ ORDER_CREATED     │
                    │ PAYMENT_COMPLETED │
                    │ ORDER_SHIPPED      │
                    │ PRODUCT_VIEWED     │
                    └─────────┬─────────┘
                              │
                              ▼
                     Spark Streaming
                    filtering/routing
                              │
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
      Kafka: orders    Kafka: payments  Kafka: shipments
             │                │                │
       ┌─────┴─────┐    ┌─────┴─────┐    ┌─────┴─────┐
       ▼           ▼    ▼           ▼    ▼           ▼
     CG-A        CG-B  CG-C        CG-D  CG-E        CG-F
       │           │    │           │    │           │
       └───────────┴────┴───────────┴────┴───────────┘
                              │
                              ▼
                         Spark jobs
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              Stream-static       Stream-stream
                  joins                joins
                    │                   │
                    └─────────┬─────────┘
                              ▼
                         PostgreSQL
                              │
                              ▼
                          Dashboard


                                │
                                ▼
                           PRODUCT_VIEWED
                                │
                                ▼
                        product_views topic

orders(ORDER CREATED)
---------------------


{
  "event_id": "evt-100001",
  "event_type": "ORDER_CREATED",
  "event_time": "2026-09-01T10:05:23.412Z",
  "order_id": "ORD-10001",
  "customer_id": "CUST-1001",
  "product_id": "PROD-501",
  "quantity": 2,
  "amount": 2499.00,
  "currency": "INR",
  "store_id": "STORE-12"
}

payments(PAYMENT_COMPLETED)
---------------------------

{
  "event_id": "evt-100002",
  "event_type": "PAYMENT_COMPLETED",
  "event_time": "2026-09-01T10:06:11.127Z",
  "payment_id": "PAY-70001",
  "order_id": "ORD-10001",
  "customer_id": "CUST-1001",
  "amount": 2499.00,
  "currency": "INR",
  "payment_method": "CREDIT_CARD",
  "status": "SUCCESS"
}

shipments(ORDER_SHIPPED)
------------------------

{
  "event_id": "evt-100003",
  "event_type": "ORDER_SHIPPED",
  "event_time": "2026-09-01T14:32:47.821Z",
  "shipment_id": "SHIP-80001",
  "order_id": "ORD-10001",
  "customer_id": "CUST-1001",
  "carrier": "DHL",
  "warehouse_id": "WH-07",
  "shipping_method": "EXPRESS"
}

views(PRODUCT_VIEWED)
---------------------

{
  "event_id": "evt-100004",
  "event_type": "PRODUCT_VIEWED",
  "event_time": "2026-09-01T10:03:12.553Z",
  "customer_id": "CUST-1001",
  "product_id": "PROD-501",
  "session_id": "SES-90001",
  "device": "MOBILE",
  "page": "PRODUCT_DETAIL"
}

