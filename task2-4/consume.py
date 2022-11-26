from confluent_kafka import Consumer
from collections import deque
import shelve
import json
from dateutil.parser import parse
from configparser import ConfigParser
from argparse import ArgumentParser, FileType
from get_catalog_table import get_catalog
import reproduce
import datetime
import time

class MA:
    '''class for calculating moving average'''
    def __init__(self, window=50, config_parser={}, omit_history=False):
        '''
        initialises the object for moving average calculation.
        omit_history skips the usage of previous data for calculation of moving average between successive consumer runs. This utilises the state store in a pickle serialised file and does not come into play unless consumer has a state left from previous run.
        '''
        if not omit_history:
            self.ma_history = shelve.open(config_parser['ma_history'][f'ma{window}_filename'], writeback=True)
            self.queue = self.ma_history.get(f'ma{window}') if self.ma_history.get(f'ma{window}') is not None else deque(maxlen=window)
        else:
            self.ma_history = shelve.open(config_parser['ma_history'][f'ma{window}_filename'], writeback=True)
            self.queue = deque(maxlen=window) 
        self.window = window
        self.q_sum = sum([x[3] for x in self.queue])      

    def push(self, value):
        '''calculates the moving average by maintaining a double ended queue'''
        if len(self.queue) == self.window:
            self.q_sum -= self.queue[0][3]
        self.queue.append(value)
        self.q_sum += value[3]
        ma = self.q_sum / len(self.queue)
        value.append(ma)
        # print(f"value: {value}")
        return value

class EMA:
    '''class for calculating exponential moving average'''
    def __init__(self, window=50) -> None:
        self.factor = 2/(window+1)
        self.prev_ema = 0
    def calculate(self, value):
        # formula applied for EMA
        curr_ema = self.factor * (value[3] - self.prev_ema) + self.prev_ema
        value.append(curr_ema)
        self.prev_ema = curr_ema
        return value

def main(config_parser, args):
    try:
        if args.omit_ma_history:
            s = MA(50, config_parser, True)
            t = MA(100, config_parser, True)
        else:
            s = MA(50, config_parser)
            t = MA(100, config_parser)

        u = EMA(50)

        # initialise consumer app
        c = Consumer({
        'bootstrap.servers': config_parser['default']['bootstrap.servers'],
        'group.id': config_parser['consumer']['group.id'],
        'auto.offset.reset': config_parser['consumer']['auto.offset.reset']
        })
        c.subscribe([config_parser['topic']['inflow']])

        # get datapoint template from glue catalog
        placeholder_dpoint = get_catalog(conf=config_parser)

        # poll topic for new messages
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            # deserialise message
            val = json.loads(msg.value().decode('utf-8'))

            # convert string risk score to floating point value
            val[3] = float(val[3] or 0)

            '''logic for task 2'''
            # modified_row = s.push(val)
            # dpoint = column_mapper(modified_row, placeholder_dpoint)
            # reproduce.main(config_parser, dpoint)
            '''logic for task 3'''
            # modified_row = s.push(val)
            # modified_row_2 = t.push(modified_row)
            # dpoint = column_mapper(modified_row_2, placeholder_dpoint)
            # reproduce.main(config_parser, dpoint)
            '''logic for task 4'''
            modified_row = u.calculate(val)
            modified_row_2 = t.push(modified_row)
            dpoint = column_mapper(modified_row_2, placeholder_dpoint)
            reproduce.main(config_parser, dpoint)

    except KeyboardInterrupt:        
        c.close()
    except Exception as e1:
        print(msg.value().decode('utf-8'))
        raise e1
    finally:
        s.ma_history[f'ma{s.window}'] = s.queue
        t.ma_history[f'ma{t.window}'] = t.queue
        s.ma_history.close()
        t.ma_history.close()
        c.close()

def column_mapper(modified_row, placeholder_dpoint):
    '''puts data values into their placeholder in datapoint templates'''
    ind = 0
    for i in placeholder_dpoint['payload'].keys():
        if i.startswith('_'):
            continue
        placeholder_dpoint['payload'][i] = modified_row[ind]
        ind+=1
    placeholder_dpoint['payload']['_modified_ts'] = time.time_ns()
    return placeholder_dpoint

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--omit_ma_history')
    args = parser.parse_args()

    config_parser = ConfigParser()    
    config_parser.read_file(args.config_file)
    main(config_parser, args)
