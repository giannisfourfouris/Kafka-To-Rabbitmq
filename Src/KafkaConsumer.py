import yaml
from confluent_kafka import Consumer, KafkaException, KafkaError
from RabbitMqProducer import *
import argparse

class KafkaConsumer:
    def __init__(self, config_file):
        self.config_file = config_file
        with open(self.config_file, 'r') as file:
                config = yaml.safe_load(file)['kafka']
        
        self.bootstrap_servers = config['bootstrap_servers']
        self.sasl_username = config['sasl_username']
        self.sasl_password = config['sasl_password']
        self.security_protocol = config['security_protocol']
        self.sasl_mechanism = config['sasl_mechanism']
        self.group_id = config['group_id']
        self.topic = config['topic']
        self.batch_size = config['batch_size']
        self.timeout = config['timeout']
        self.min_commit_count = config['min_commit_count']
        self.enable_auto_commit = config['enable_auto_commit']
        self.auto_offset_reset = config['auto_offset_reset']
        self.commit_count = 0


        if 'localhost' in self.bootstrap_servers:
            self.conf = {'bootstrap.servers': self.bootstrap_servers,                                                              
            'group.id': self.group_id,
            'enable.auto.commit': self.enable_auto_commit,
            'auto.offset.reset': self.auto_offset_reset}

        else:
            self.conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'security.protocol': self.security_protocol,
                'sasl.mechanism': self.sasl_mechanism,
                'sasl.username': self.sasl_username,
                'sasl.password': self.sasl_password,
                'group.id': self.group_id,
                'enable.auto.commit': self.enable_auto_commit,
                'auto.offset.reset': self.auto_offset_reset
            }

        self.consumer = Consumer(self.conf)

        self.consumer.subscribe([self.topic])
    
    def consume_messages(self):
        try:
            rabbit_mq_producer = RabbitMqProducer(self.config_file)
            print('[Kafka] [*] Waiting for messages. To exit press CTRL+C')
            while True:
                messages = self.consumer.consume(self.batch_size, timeout=self.timeout)

                if messages is None or not messages:
                    continue

                for msg in messages:
                    if msg.error():
                        print(f"[Kafka] Error consuming message: {msg.error()}")
                    else:
                        value = msg.value().decode('utf-8')
                        print(f"[Kafka] [x] Received {value}")
                        rabbit_mq_producer.produce_message(value)

                    self.commit_count += 1
                    if self.enable_auto_commit == 'false' and self.commit_count >= self.min_commit_count and self.min_commit_count >= 1:
                        self.consumer.commit()
                        self.commit_count = 0

        except KeyboardInterrupt:
            print("Aborted by user")
        finally:
            self.consumer.close()


if __name__ == "__main__":
        parser = argparse.ArgumentParser(description='Choose config')
        parser.add_argument('--config', type=str, default='config.yaml', help='Path to the configuration file (default: config.yaml)')
        args = parser.parse_args()

        consumer = KafkaConsumer(args.config)
        consumer.consume_messages()