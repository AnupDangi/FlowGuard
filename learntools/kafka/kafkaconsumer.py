from confluent_kafka import Consumer
import json

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumers", # consumer group id
    "auto.offset.reset": "earliest" # start from the beginning if no offset is committed
}

consumer = Consumer(conf)
consumer.subscribe(["orders"])


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(" Error:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print(
            f"user={event['user_id']} "
            f"amount={event['amount']} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

        consumer.commit(msg) ## commit the offset after processing the message

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
