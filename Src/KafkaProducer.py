from confluent_kafka import Producer
import yaml
import socket
import json
import argparse

class KafkaProducer:
    def __init__(self, config_file):
        self.config_file = config_file
        with open(self.config_file, 'r') as file:
            config = yaml.safe_load(file)['kafka']

        self.bootstrap_servers = config['bootstrap_servers']
        self.sasl_username = config['sasl_username']
        self.sasl_password = config['sasl_password']
        self.security_protocol = config['security_protocol']
        self.sasl_mechanism = config['sasl_mechanism']
        #self.client_id = config['client_id']
        self.topic = config['topic']
        

        # self.conf = {
        #     'bootstrap.servers': self.bootstrap_servers,
        #     'security.protocol': self.security_protocol,
        #     'sasl.mechanism': self.sasl_mechanism,
        #     'sasl.username': self.sasl_username,
        #     'sasl.password': self.sasl_password,
        #     'client.id': socket.gethostname()
        # }

        self.conf = {'bootstrap.servers': self.bootstrap_servers,
        'client.id': socket.gethostname()}

        self.producer = Producer(self.conf)

    def produce_messages(self, data, threshold):
        
        def acked(err, msg):
            if err is not None:
                print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                print("Message produced: %s" % (str(msg)))
            
        for index, (key, value) in enumerate(data, start=1):
            # Convert data to JSON
            jsonData = json.dumps(value, indent=4)

            self.producer.produce(self.topic, key=key, value=jsonData, callback=acked)
            self.producer.flush()

            if index % threshold == 0:
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Choose config')
    parser.add_argument('--config', type=str, default='config.yaml', help='Path to the configuration file (default: config.yaml)')
    args = parser.parse_args()
    

    # number of messages to send
    num_of_sent_messages = 2

    # Define the file path where the JSON data is stored
    file_Path = "../Sample Data/totalSampleData.json"

    # Open the specified file in read mode, specifying utf-8 encoding
    with open(file_Path, 'r', encoding='utf-8') as file:
        # Load the JSON data from the file into a Python dictionary
        data = json.load(file)

    consumer = KafkaProducer(args.config)
    consumer.produce_messages(data, num_of_sent_messages)