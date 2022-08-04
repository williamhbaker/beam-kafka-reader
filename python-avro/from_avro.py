# run like:
#   CONSUMER_TOPICS=hello-world BOOTSTRAP_SERVERS=192.168.2.47:9092 REGISTRY_URL=http://192.168.2.47:8081 python from_avro.py

# based on https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_consumer.py & https://github.com/sohailRa/beamAvroPubSub

import apache_beam as beam
import logging
from apache_beam.io.external.kafka import ReadFromKafka
import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class User(object):
    """
    User record
    Args:
        name (str): User's name
        favorite_number (int): User's favorite number
        favorite_color (str): User's favorite color
    """

    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.
    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """
    if obj is None:
        return None

    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


logging.getLogger().setLevel(logging.WARN)

pipeline_options = beam.options.pipeline_options.PipelineOptions(
    runner="Direct",
    streaming=True,
)

# This is the schema the consumer will use for deserialization.
path = os.path.realpath(os.path.dirname(__file__))
with open(f"{path}/user.avsc") as f:
    schema_str = f.read()

# The schema registry client will be used to get the schema used by the producer to encode the data.
sr_conf = {'url': os.getenv("REGISTRY_URL")}
schema_registry_client = SchemaRegistryClient(sr_conf)

# Creating a deserializer requires both the consumer schema, and the schema registry client to look
# up schemas used by the producer.
avro_deserializer = AvroDeserializer(
    schema_registry_client, schema_str, dict_to_user)

p = beam.Pipeline(options=pipeline_options)

_ = (
    p
    | "read" >> ReadFromKafka(
        consumer_config={
            'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
            'auto.offset.reset': "earliest",
        },
        topics=[os.getenv("CONSUMER_TOPICS")],
        # Needed workaround for demo, see https://github.com/apache/beam/issues/20979
        max_num_records=10,
    )
    # Python beam SDK does not support schema registry, so we have to deserialize manually in this
    # hacky way, see https://github.com/apache/beam/issues/21225
    | "deserialize" >> beam.Map(lambda x: avro_deserializer(x[1], None))
    | "output" >> beam.Map(lambda x: print(x.name, x.favorite_number, x.favorite_color))
)

result = p.run()
result.wait_until_finish()
