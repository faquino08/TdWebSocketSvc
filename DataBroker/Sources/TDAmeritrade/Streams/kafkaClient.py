from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

class kafkaClient:
    def __init__(self,topics=['TdActivesByShares','TdActivesByTrades'],groupid='mygroup',kafkaAddress='10.6.47.45:9092'):
        self.topics = topics
        sr_conf = {'url': f'http://{kafkaAddress}'}
        schema_registry_client = SchemaRegistryClient(sr_conf)

        avro_deserializer = AvroDeserializer(schema_registry_client)
        string_deserializer = StringDeserializer('utf_8')

        consumer_conf = {'bootstrap.servers': f'{kafkaAddress}',
                            'key.deserializer': string_deserializer,
                            'value.deserializer': avro_deserializer,
                            'group.id': groupid,
                            'auto.offset.reset': "latest"}

        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe(self.topics)
        return