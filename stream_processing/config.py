import redis
rds = redis.Redis(host='localhost', port=6579, decode_responses=True) # rds is an instance of redis.Redis()

TWEET = "tweet"    # on stream named "tweet"
WORD_PREFIX = "w_" # two stream w_0, w_1
WORD_BUCKETS = 2   

WORDSET = "words"   # set name (a data structure by redis)