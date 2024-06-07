import pika
import yaml

class RabbitMqProducer:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)['rabbitmq']
        self.host = config['host']
        self.port = config['port']
        self.username = config['username']
        self.password = config['password']
        self.virtual_host = config['virtual_host']
        self.erase_on_connect = config['erase_on_connect']
        self.queue_name = config['queue_name']
        self.durable = config['durable']
        self.exchange_type = config['exchange_type']
        self.exchange_name = config['exchange_name']
        self.message_id = 0

    def produce_message(self, body):
        # If you want to have a more secure SSL authentication, use ExternalCredentials object instead
        credentials = pika.PlainCredentials(username=self.username, password=self.password, erase_on_connect=self.erase_on_connect)
        parameters = pika.ConnectionParameters(host=self.host, port=self.port, virtual_host=self.virtual_host, credentials=credentials)
        # We are using BlockingConnection adapter to start a session. It uses a procedural approach to using Pika and has most of the asynchronous expectations removed
        connection = pika.BlockingConnection(parameters)
        # A channel provides a wrapper for interacting with RabbitMQ
        channel = connection.channel()
        # Check for a queue and create it, if necessary
        channel.queue_declare(queue=self.queue_name, durable=self.durable)

        if self.exchange_type != '':
            channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)

        prop = pika.BasicProperties(
            message_id=str(self.message_id ),
            priority= 0,
            delivery_mode=2,
            headers={'bar': 'baz'},
            content_type='application/json'
        )

        # For the sake of simplicity, we are not declaring an exchange, so the subsequent publish call will be sent to a Default exchange that is predeclared by the broker
        channel.basic_publish(exchange=self.exchange_name, properties= prop ,routing_key=self.queue_name, body=body)
        print(f"[Rabbitmq] [x] Sent {body}")
        self.message_id +=1
        # Safely disconnect from RabbitMQ
        connection.close()
