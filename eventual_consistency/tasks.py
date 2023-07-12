import string
from celery import Celery
from config import IPS
import socket
import json 
from redis import Redis, WatchError, ConnectionError, TimeoutError
from random import randint
from time import sleep
from config import IPS

broker = f'pyamqp://test:test@{IPS[0]}'
app = Celery('tasks', backend='rpc', broker=broker)

"""
Inserts the dict corresponding to a file into stream of atleast two redis instances
"""
@app.task(acks_late=True, ignore_results=True, bind=True, max_retries=3)
def mapper(self, filename: string, idx: int):
    curr_ip = socket.gethostbyname(socket.gethostname())
    conns = [Redis(host=ip, decode_responses=True, socket_timeout=10) for ip in IPS]
    # curr_rds = Redis(host=curr_ip, decode_responses=True, socket_timeout=10)

    # Generating dictionary corresponding to words present in supplied file
    wc = {}
    with open(filename, mode='r') as f:
        for text in f:
            if text == '\n':
                continue
            ## remove first 3 fields and last two fields
            ## tweet itself can have commas
            sp = text.split(',')[4:-2]
            tweet = " ".join(sp)
            for word in tweet.split(" "):
                word = word.lower()
                if word not in wc:
                    wc[word] = 0
                wc[word] = wc[word] + 1

    # Add this data to streams of atleast 2 nodes
    suc_inserts = []
    while len(suc_inserts) < 2:
        for conn in conns:
            if conn in suc_inserts:
                continue
            try:
                stream_data_id = conn.xadd(name='mystream', fields={idx: json.dumps(wc)})
                conn.set(idx, stream_data_id)
                conn.setbit('is_present', idx, 1)
                suc_inserts.append(conn)
            except (ConnectionError, TimeoutError):
                continue
    return 0
