from confluent_kafka import Consumer
from collections import deque
import json

class MA:
    def __init__(self, window=50):
        self.queue = deque(maxlen=window)
        self.window = window
        self.q_sum = 0        
    def push(self, value):
        if len(self.queue) == self.window:
            self.q_sum -= self.queue[0][3]
        self.queue.append(value)
        self.q_sum += value[3]
        ma = self.q_sum / len(self.queue)
        value.append(ma)
        print(f"value: {value}, MA: {ma}")
try:
    s = MA(50)
    c = Consumer({
    'bootstrap.servers': "b-2.honestcluster.mivhy0.c2.kafka.ap-south-1.amazonaws.com:9092,b-1.honestcluster.mivhy0.c2.kafka.ap-south-1.amazonaws.com:9092,b-3.honestcluster.mivhy0.c2.kafka.ap-south-1.amazonaws.com:9092",
    'group.id': 'con_group_test_2',
    'auto.offset.reset': 'earliest'
    })
    c.subscribe(['test_topic_2'])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        val = json.loads(msg.value().decode('utf-8'))
        val[3] = float(val[3] or 0)
        s.push(val)
except KeyboardInterrupt:        
    c.close()
except Exception as e1:
    print(msg.value().decode('utf-8'))
    raise e1
finally:
    c.close()