import sys
import os
import time
import sys
import random
import tasks
from celery import chord, group
from config import rds, TWEET, WORDSET

if len(sys.argv) < 2:
    print ('Use the command: python3 client.py <data_dir>')

DIR = sys.argv[1]
rds.delete(WORDSET)
rds.xtrim(TWEET, 0)  # tweet is a redis stream and pass 0 to trim, which mean max length of stream is 0.
rds.xtrim('w_0', 0)
rds.xtrim('w_1', 0)

# ------ Adding data to the stream

cnt = 0
for (pth, dirs, files) in os.walk(DIR):
    for f in files:
        abs_file = os.path.join(pth, f)
        for line in open(abs_file, 'r'):
            rds.xadd(TWEET, {TWEET: line})


# ---------Testing code output
# ctr = 0
# while True:
#     print (rds.zrevrangebyscore(WORDSET, '+inf', '-inf', 0, 10, withscores=True))
#     print (f'-- {ctr} ---')
#     time.sleep(1)
#     ctr = ctr + 1
#     if ctr >= 1000:
#         break

