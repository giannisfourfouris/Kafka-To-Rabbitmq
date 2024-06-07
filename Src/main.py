from KafkaConsumer import *
from RabbitMqConsumer import *
import argparse

if __name__ == "__main__":
        parser = argparse.ArgumentParser(description='Choose consumer type')
        parser.add_argument('consumer_type', type=str, choices=['rabbitmq', 'kafka'], help='Type of consumer to use (rabbitmq or kafka)')
        parser.add_argument('--config', type=str, default='config.yaml', help='Path to the configuration file (default: config.yaml)')
        args = parser.parse_args()

        if args.consumer_type == 'kafka':
                consumer = KafkaConsumer(args.config)
                consumer.consume_messages()
        elif args.consumer_type == 'rabbitmq':
                consumer = RabbitMqConsumer(args.config)
                consumer.consume_messages()