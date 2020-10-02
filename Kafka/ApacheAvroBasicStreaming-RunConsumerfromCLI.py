import asyncio
from dataclasses import asdict, dataclass, field
import io
import json
import random
from confluent_kafka import Producer
from faker import Faker
from fastavro import parse_schema, writer


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    schema = parse_schema({
        "type":"record",
        "name":"streaming-data",
        "namespace":"apache avro",
        "fields": [
                {"name": "email", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "uri", "type": "string"},
                {"name": "number", "type": "int"},
            ],
    })
    
    def serialize(self):
        """Serializes the ClickEvent for sending to Kafka"""
        #       See: https://fastavro.readthedocs.io/en/latest/schema.html?highlight=parse_schema#fastavro-schema
        #       See: https://docs.python.org/3/library/dataclasses.html#dataclasses.asdict
        #       See: https://docs.python.org/3/library/io.html?highlight=bytesio#io.BytesIO
        parser=ClickEvent.schema
        out = io.BytesIO()
        writer(out, parser, [asdict(self)])
        return out.getvalue()

async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    print(BROKER_URL)
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("avro-streaming"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    await asyncio.create_task(produce(topic_name))


if __name__ == "__main__":
    main()
