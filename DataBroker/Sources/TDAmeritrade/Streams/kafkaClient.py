from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

class kafkaClient:
    def __init__(self,topics=['TdActivesByShares','TdActivesByTrades'],groupid='mygroup',kafkaAddress='10.6.47.45:9092',schemaRegAddress='10.6.47.45:8081'):
        '''
        Class representing a client to connect to Kafka cluster and consume records.
        topics              -> (list) Names of topics to consume
        groupid             -> (str) Group id of the kafka consumer
        kafkaAddress        -> (str) IP Address or URL of Kafka cluster
        '''
        self.topics = topics
        sr_conf = {'url': f'http://{schemaRegAddress}'}
        schema_registry_client = SchemaRegistryClient(sr_conf)

        self.avro_deserializer = AvroDeserializer(schema_registry_client)
        #string_deserializer = StringDeserializer('utf_8')

        consumer_conf = {'bootstrap.servers': f'{kafkaAddress}',
                            #'key.deserializer': string_deserializer,
                            #'value.deserializer': avro_deserializer,
                            'group.id': groupid,
                            'auto.offset.reset': "latest"}

        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe(self.topics)
        return