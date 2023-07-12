from abc import ABC
from ast import List, Tuple
import redis
import socket
import json
from operator import itemgetter
 
# Abstract base class 
class Lab3Redis(ABC):
  """
  __init__ accepts a list of IP addresses on which redis is deployed.
  Number of IPs is typically 3.
  """

  def __init__(self, ips: List):
    self.conns = [redis.Redis(host=ip, socket_timeout=100) for ip in ips]
    self.num_instances = len(ips)
    self.all_ips = ips
    for conn in self.conns:
      conn.flushall()
      id = conn.xadd('mystream', {'foo': 0})
      conn.xdel('mystream', id)
      
  def get_top_words(self, n: int, repair: bool = False) -> List:
    pass

class ConsistentRedis(Lab3Redis):

  def get_top_words(self, n: int,
                    repair: bool = False) -> List:
    pass

class AvailableRedis(Lab3Redis):
  """
  This method is necessary for evaluation
  """

  def get_top_words(self, n: int, repair: bool = False) -> List:
    curr_ip = socket.gethostbyname(socket.gethostname())
    curr_rds = redis.Redis(host=curr_ip, socket_timeout=100)

    if repair:
      # compare the streams B and C with A to bring them in sync:
      # 1. compare the stream pairs A->B and A->C and store the data in stream A
      # 2. Once all are in sync iterate over all files in current stream and compute dict 

      # Healing redis instances one-by-one
      def heal_redis_instance(node):
        nodes = [0, 1, 2]
        nodes.remove(node)
        curr_rds = self.conns[node]
        rds_1 = self.conns[nodes[0]]
        rds_2 = self.conns[nodes[1]]
        try:
          present_1 = rds_1.get('is_present')
          for i in range(0, len(present_1) * 8):
            curr_rds.setbit('bitmap1', i, rds_1.getbit('is_present', i))
        except (ConnectionError, TimeoutError):
          pass

        try:
          present_2 = rds_2.get('is_present')
          for i in range(0, len(present_2) * 8):
            curr_rds.setbit('bitmap2', i, rds_2.getbit('is_present', i))
        except (ConnectionError, TimeoutError):
          pass

        curr_rds.bitop('xor', 'tmp_bitmap1', 'is_present', 'bitmap1')
        curr_rds.bitop('and', 'transfers1', 'tmp_bitmap1', 'bitmap1')
        num_bits = len(curr_rds.get('transfers1')) * 8
        to_fetch = []
        try:
          for i in range(0, num_bits):
            if curr_rds.getbit('transfers1', i) == 1 and rds_1.getbit('is_present', i) == 1 and curr_rds.getbit('is_present', i) == 0:
              to_fetch.append(i)
          for f in to_fetch:
            print('file', f)
            stream_key = rds_1.get(f)
            file_data = rds_1.xrange('mystream', min = stream_key, max = stream_key)
            stream_data_id = curr_rds.xadd('mystream', file_data[0][1])
            curr_rds.setbit('is_present', f, 1)
            curr_rds.set(f, stream_data_id)
        except (ConnectionError, TimeoutError):
          pass
        
        curr_rds.bitop('or', 'transfers1', 'is_present', 'transfers1')
        curr_rds.bitop('xor', 'tmp_bitmap2', 'transfers1', 'bitmap2')
        curr_rds.bitop('and', 'transfers2', 'tmp_bitmap2', 'bitmap2')
        num_bits = len(curr_rds.get('transfers2')) * 8
        to_fetch = []
        try:
          for i in range(0, num_bits):
            if curr_rds.getbit('transfers2', i) == 1 and rds_2.getbit('is_present', i) == 1 and curr_rds.getbit('is_present', i) == 0:
              to_fetch.append(i)
          for f in to_fetch:
            print('file', f)
            stream_key = rds_2.get(f)
            file_data = rds_2.xrange('mystream', min = stream_key, max = stream_key)
            stream_data_id = curr_rds.xadd('mystream', file_data[0][1])
            curr_rds.setbit('is_present', f, 1)
            curr_rds.set(f, stream_data_id)
        except (ConnectionError, TimeoutError):
          pass

      heal_redis_instance(0)
      heal_redis_instance(1)
      heal_redis_instance(2)
      

    to_process = curr_rds.xrange('mystream', min = '-', max = '+')
    stream_len = curr_rds.xlen('mystream')
    print(stream_len)

    # Creating visited dict to track of already computed files
    visited = {}
    # Result for storing the final results
    result = {}
    for stream in to_process:
      for id, d in stream[1].items():
        if id in visited:
          continue
        visited[id] = 1
        for key,value in json.loads(d).items():
          if key not in result:
              result[key] = 0
          result[key] += int(value)
    # curr_rds.zadd('result', result)
    # res = curr_rds.zrevrangebyscore('result', '+inf', '-inf', 0, 10, withscores=True)
    res = dict(sorted(result.items(), key = itemgetter(1), reverse = True)[:10])
    return res
