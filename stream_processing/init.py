
# --- import
from celery import chord, group
from config import rds, TWEET, WORDSET, WORD_PREFIX, WORD_BUCKETS
import tasks

## Configure consumer groups and workers
def setup_stream(stream_name: str):     # seems like there is no function to create a stream rather than entering a value.
  garbage = {"a": 1} # dictionary : key, value pair   |  Create the stream by adding garbage values
  id = rds.xadd(stream_name, garbage)   # creading a stream which returns you an "unique id"
  rds.xdel(stream_name, id)

#------- Setup Stream (Tweet, w_0, w_1)
rds.flushall()                          # flusall() redis, nothing remain in memory now...
setup_stream(TWEET)                     # set_up code for setting stream ... 
for i in range(WORD_BUCKETS):
  stream_name = f"{WORD_PREFIX}{i}"
  setup_stream(stream_name)

# ----- General Utility functions
def create_group(streamname, groupname):
    # Creates a customer group for the stream stored at streamname
    # If you want group to fetch the stream from beginning use id='0'
    return rds.xgroup_create(streamname, groupname, id='0', mkstream=False) # returns Ok (a string)

# destroy the group    
def destroy_group(streamname, groupname):
    return rds.xgroup_destroy(streamname, groupname)
    

# returns a list of unique consumers name.
def create_name_list(no_of_consumers):
    
    consumer_list = []
    for i in range(no_of_consumers):
        consumer_list.append('c' + str(i)) # c0, c1, c2, ..., 
    return consumer_list

def register_stream(streamname, groupname, consumer_list):
    for consumer in consumer_list:
        val = rds.xgroup_createconsumer(streamname, groupname, consumer)


#---- setup VARIABLES ---
GROUPNAME1 = 'producer'
GROUPNAME2 = 'consumer'
W0 = 'w_0'
W1 = 'w_1'

# --- just for testing ----
rds.delete(WORDSET)
rds.xtrim(TWEET, 0)  # tweet is a redis stream and pass 0 to trim, which mean max length of stream is 0.
rds.xtrim('w_0', 0)
rds.xtrim('w_1', 0)

# # ----- Destroying group 
destroy_group(TWEET, GROUPNAME1)
destroy_group(W0, GROUPNAME2)
destroy_group(W1, GROUPNAME2)


#----- Setup of Celery workers for Tokenizer -------
no_of_tokenizer = 3
count = 5000 
create_group(TWEET, GROUPNAME1)
tokenizer_list = create_name_list(no_of_tokenizer) # [c0, c1, ...] basedon no_of_tokenizer
register_stream(TWEET, GROUPNAME1, tokenizer_list)
group(tasks.tokenize_lines.s(TWEET, GROUPNAME1, tokenizer, count, no_of_tokenizer) for tokenizer in tokenizer_list)()


#----- Setup of Celery workers for Consumers --------
no_of_consumers = 5
count = 2 
consumer_list = create_name_list(no_of_consumers)
create_group(W0, GROUPNAME2)                  
create_group(W1, GROUPNAME2)
register_stream(W0, GROUPNAME2, consumer_list)    
register_stream(W1, GROUPNAME2, consumer_list)
group(tasks.aggregate_result.s(W0, W1, GROUPNAME2, consumer, count, no_of_consumers) for consumer in consumer_list)()
