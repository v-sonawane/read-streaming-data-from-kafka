import apache_beam as beam
from confluent_kafka import Consumer, KafkaError
import logging

class KafkaConsumerDoFn(beam.DoFn):
    def __init__(self, config):
        super(KafkaConsumerDoFn, self).__init__()
        self._config = config

    def start_bundle(self):
        self._consumer = Consumer(self._config)
        self._consumer.subscribe(['please-work'])

    def process(self, element):
        while True:
            msg = self._consumer.poll(60)
            if msg is None:
                return
            if not msg.error():
                yield msg.value().decode('utf-8')
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print('Error: %s' % msg.error())

    def finish_bundle(self):
         self._consumer.close()

def run():
    with beam.Pipeline() as p:
        kafka_config = {
        'bootstrap.servers': '172.25.157.44:9092',
        'group.id': 'bappa-morya',
        'auto.offset.reset': 'earliest'
    }
        kafka_messages = (
            p
            | 'Create input' >> beam.Create([None])
            | 'Read from Kafka' >> beam.ParDo(KafkaConsumerDoFn(kafka_config))
    )
        kafka_messages | 'Save to text file' >> beam.io.WriteToText('output.txt')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
