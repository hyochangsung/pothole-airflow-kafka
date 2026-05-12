import json
import os
import time
from confluent_kafka import Consumer, KafkaException


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "pothole-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "pothole-consumer-group")


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })


def main():
    consumer = create_consumer()
    consumer.subscribe([TOPIC])

    print(f"Consumer started. topic={TOPIC}, bootstrap={BOOTSTRAP_SERVERS}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            raw_value = msg.value().decode("utf-8")

            try:
                data = json.loads(raw_value)
            except json.JSONDecodeError:
                data = {"raw": raw_value}

            print("received:", data)

    except KeyboardInterrupt:
        print("consumer stopped")

    finally:
        consumer.close()


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print("consumer error:", e)
            print("retry after 5 seconds")
            time.sleep(5)
