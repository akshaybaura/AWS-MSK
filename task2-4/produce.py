from confluent_kafka import Producer
import socket
import gzip
import uuid
import csv
import json
from dateutil.parser import parse
from configparser import ConfigParser
from argparse import ArgumentParser, FileType


def main(config_parser, yr):
    #initialise producer
    conf = {
        'bootstrap.servers': config_parser['default']['bootstrap.servers'],
        'client.id': socket.gethostname()
        }
    producer = Producer(conf)

    #open and readgzip file
    with gzip.open('rejected_2007_to_2018Q4.csv.gz', mode="rt") as f:
        csvobj = csv.reader(f,delimiter = ',',quotechar='"')
        next(csvobj, None)
        for line in csvobj:
            try:
                year = parse(line[1], fuzzy=False).year
            except:
                print('Error in row for application_date. Skipped row: ', line)
            else:
                #check if current row lies in the range provided with input
                start, end = list(map(int, yr.split('-')))
                yr_list = [i for i in range(start, end+1)]
                if year in yr_list:
                    #produce record to topic
                    producer.produce(config_parser['topic']['inflow'], key=str(uuid.uuid4()), value=json.dumps(line).encode('utf-8'))
                    producer.flush()

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--year')
    args = parser.parse_args()

    config_parser = ConfigParser()    
    config_parser.read_file(args.config_file)
    main(config_parser, args.year)