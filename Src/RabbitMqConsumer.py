import pika
import yaml
import argparse

class RabbitMqConsumer:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)['rabbitmq']
        self.host = config['host']
        self.port = config['port']
        self.username = config['username']
        self.password = config['password']
        self.virtual_host = config['virtual_host']
        self.queue_name = config['queue_name']
        self.durable = config['durable']
        self.exchange_type = config['exchange_type']
        self.exchange_name = config['exchange_name']
    

    def consume_messages(self):
        credentials = pika.PlainCredentials(username=self.username, password=self.password)
        parameters = pika.ConnectionParameters(host=self.host, port=self.port, virtual_host=self.virtual_host, credentials=credentials)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=self.durable)
        
        if self.exchange_type != '':
            channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name)
        

         # Since RabbitMQ works asynchronously, every time you receive a message, a callback function is called. We will simply print the message body to the terminal 
        def callback(ch, method, properties, body):
            print(f"[Rabbitmq] [x] Received {body.decode('utf-8')}")

        # Consume a message from a queue. The auto_ack option simplifies our example, as we do not need to send back an acknowledgement query to RabbitMQ which we would normally want in production
        channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
        print('[Rabbitmq]  [*] Waiting for messages. To exit press CTRL+C')

        try:
            # Start listening for messages to consume
            channel.start_consuming()
        except KeyboardInterrupt:
            print("Interrupted")


if __name__ == "__main__":
        parser = argparse.ArgumentParser(description='Choose config')
        parser.add_argument('--config', type=str, default='config.yaml', help='Path to the configuration file (default: config.yaml)')
        args = parser.parse_args()

        consumer = RabbitMqConsumer(args.config)
        consumer.consume_messages()