from confluent_kafka import Producer
import socket
import gzip
import uuid
import csv
import json
conf = {'bootstrap.servers': "b-2.honestcluster.mivhy0.c2.kafka.ap-south-1.amazonaws.com:9092,b-1.honestcluster.mivhy0.c2.kafka.ap-south-1.amazonaws.com:9092,b-3.honestcluster.mivhy0.c2.kafka.ap-south-1.amazonaws.com:9092",'client.id': socket.gethostname()}

producer = Producer(conf)

with gzip.open('rejected_2007_to_2018Q4.csv.gz', mode="rt") as f:
    csvobj = csv.reader(f,delimiter = ',',quotechar='"')
    next(csvobj, None)
    for line in csvobj:
        producer.produce('test_topic_2', key=str(uuid.uuid4()), value=json.dumps(line).encode('utf-8'))
        producer.flush()