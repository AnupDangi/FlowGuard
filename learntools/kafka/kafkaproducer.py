from confluent_kafka import Producer
import json
import time
import random
## conf is the configuration of the kafka producer
conf={ 
    "bootstrap.servers": "localhost:9092",
    "acks":"all" # leader waits for all replicas to acknowlege message
}

## remember theese producers are async in nature 

producer=Producer(conf)

def delivery_report(err,msg):
    """
        Callback function called once for each message if delivery succeeded or failed
    """

    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# ## a dummy event to be sent from producer to consumer
# event={
#     "user_id":42,
#     "event_type":"order_created",
#     "amount":299
# }


# producer.produce(
#     topics="events",
#     value=json.dumps(event),
#     on_delivery=delivery_report
# )

# ## next is partitioning the producer if we we use a key 
# ## key-> same partition for the ordered events

# producer.produce(
#     topics="events",
#     key=str(event["user_id"]), # all events for user_id 42 will go to same partition
#     value=json.dumps(event),
#     on_delivery=delivery_report
# )

# ## blocks until until all messages are send
# producer.flush() 


## infinite loop to run kakfa prodcing some events every second
while True:
    event = {
        "user_id": random.randint(1, 5),
        "event": "order_created",
        "amount": random.randint(100, 1000),
        "ts": time.time()
    }

    producer.produce(
        topic="orders",
        key=str(event["user_id"]),
        value=json.dumps(event),
        on_delivery=delivery_report
    )

    producer.poll(0)
    time.sleep(1)

