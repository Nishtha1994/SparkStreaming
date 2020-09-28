import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "streaming-data"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers":BROKER_URL})

    curr_iteration = 0
    while True:
        p.produce(TOPIC_NAME, f"iteration {curr_iteration}".encode("utf-8"))

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers":BROKER_URL,"group.id": "my-first-consumer-group"})

    c.subscribe([TOPIC_NAME])

    while True:
        message = c.poll(1.0)

        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(1)

        await asyncio.sleep(1)


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2


def main():
    """Runs the exercise"""
    client = AdminClient({"bootstrap.servers":BROKER_URL})
    topic = NewTopic(TOPIC_NAME,num_partitions=1, replication_factor=1)
    client.create_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        client.delete_topics([topic])



if __name__ == "__main__":
    main()