from confluent_kafka import Producer
import socket
import uuid
import json
from configparser import ConfigParser
from argparse import ArgumentParser, FileType


def main(config, datapoint):
    #initialise reproducer
    conf = {
        'bootstrap.servers': config['default']['bootstrap.servers'],
        'client.id': socket.gethostname()
        }
    reproducer = Producer(conf)

    #put datapoint to topic
    reproducer.produce(config['topic']['outflow'], key=str(uuid.uuid4()).encode('utf-8'), value=json.dumps(datapoint).encode('utf-8'))
    reproducer.flush()
    

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()    
    config_parser.read_file(args.config_file)
    main(config_parser)