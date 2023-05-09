import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import kafkaio

consumer_config = {"topic": "please-work",
                   "bootstrap_servers": "172.25.157.44:9092",
                   "group_id": "bappa-morya"}

with beam.Pipeline(options=PipelineOptions()) as p:
    messages = p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(
        consumer_config=consumer_config,
        value_decoder=bytes.decode,
    )
    messages | 'Saving' >>  beam.io.WriteToText('beam-nuggets-output.txt')
