
from celery import Celery, chord, group
from config import rds, TWEET, WORD_PREFIX, WORD_BUCKETS, WORDSET
import redis
import time

app = Celery('tasks', backend='redis://localhost:6579', broker='pyamqp://localhost')


# This function will check for the pending tasks for other consumers in the same group.
def claim_pending_tasks(streamname, groupname, consumername, count, threshold, no_of_consumers):
    check_consumers = []
    for i in range(no_of_consumers):  # fetch the name of other consumer(excluding self)
        cur_consumer = "c" + str(i)
        if cur_consumer != consumername:
            check_consumers.append(cur_consumer)
    min_idle_time = 95000 # 95 sec   # threshold to filter(tried from a range of 20sec to 100sec) work fine with most.
    total_claimed = 0  # total number of claimed till now.          
    res = [] # res : this will be in the same format as the xreadgroup return [['streamname', [{}, {}]]['streamname2', [{}, {}]]]
    for c in check_consumers:
        consumer_info = rds.xpending_range(name=streamname, groupname=groupname, min='-', max='+', count=count, consumername=c)
        message_ids = [] # filter the message id from the result that xpending_range() returns
        for info in consumer_info: 
            message_ids.append(info['message_id'])
        if len(message_ids): # need to check this because can't feed empty to xclaim. 
            filter_msg_id = rds.xclaim(name=streamname, groupname=groupname, consumername=consumername, min_idle_time=min_idle_time, message_ids=message_ids)
            if (len(filter_msg_id)):                        
                res.append([streamname, filter_msg_id]) # append it to the result in the correct format.
            total_claimed += len(filter_msg_id)             
        if (total_claimed > threshold): # incase it reach to the threshold 
            break   
    return res

# This task is assigned to the set of workers which are reading from the tweet stream.
@app.task(acks_late=True)
def tokenize_lines(streamname, groupname, consumername, count, no_of_tokenizer):
    while True:     # As we are getting a stream, this will keep running.
        res = rds.xreadgroup(groupname, consumername, {streamname : ">"}, count=count)  # read count number of fresh entry from streamname
        if res == []:   # in case nothing is available then check for other pending list.
            threshold = count * 2 # can change this
            res = claim_pending_tasks(streamname, groupname, consumername, count, threshold, no_of_tokenizer)
            if len(res) == 0:                                                           # if still didn't get anything try xreadgroup() one more time.
                continue
        list_ack = [] # list of all id to which we want to sent acknowledgement.
        stream_w0 = {} # all the words {word: cnt} which will go into stream w_0
        stream_w1 = {} # all the words {word: cnt} which will go into stream w_1
        for stream in res:
            stream_id = stream[0]               
            for item in stream[1]:
                msg_id = item[0]
                msg = item[1][streamname]
                list_ack.append(msg_id)  # append msg_id into ack_list.
                if (msg == '\n'):
                    continue
                sp = msg.split(',')[4:-2] 
                tweet = " ".join(sp)
                
                for word in tweet.split(" "):
                    word = word.lower()  # lower the word according to the trend.py 
                    hash_val = hash(word) % 2   
                    if hash_val == 0:
                        if word not in stream_w0.keys():
                            stream_w0[word] = 0
                        stream_w0[word] += 1
                    else:
                        if word not in stream_w1.keys():
                            stream_w1[word] = 0
                        stream_w1[word] += 1

        while True:  # This is the pipeline, we want to perform each task atomically that's we need this.
            try:
                with rds.pipeline() as pipe:
                    pipe.watch("w_0") # set watcher to w_0 and w_1 streams
                    pipe.watch("w_1")
                    pipe.multi()
                    if len(stream_w0):
                        pipe.xadd("w_0", stream_w0)
                    if len(stream_w1):
                        pipe.xadd("w_1", stream_w1)
                    if len(list_ack):
                        pipe.xack(streamname, groupname, *list_ack)
                    pipe.execute()          
                    break
            except redis.WatchError:# In case exception try this again.
                pipe.unwatch()
                pass


# This task is assigned to the workers which will process the w_0 and w_1 stream.
@app.task(acks_late=True)
def aggregate_result(stream1, stream2, groupname, consumername, count, no_of_consumers):
    while True:
        res = rds.xreadgroup(groupname, consumername, streams={stream1 : ">", stream2: ">"}, count=count) # read from both streams w_0 and w_1 count number of messages.
        if res == []: # In case if nothing is available in stream to process then go for the claim_pending_tasks, where we will be looking for other consuemrs in the same group and fetch pending tasks.  
            threshold = count # can change this for tuning
            res += claim_pending_tasks(stream1, groupname, consumername, count, threshold, no_of_consumers) # Fetch from the stream1
            res += claim_pending_tasks(stream2, groupname, consumername, count, threshold, no_of_consumers) # Fetch from the stream2
            if len(res) == 0: # If still nothing then again go for xreadgroup()
                continue
    
        while True:
            with rds.pipeline() as pipe: # In case if we have data, set the pipe
                try:
                    pipe.watch(WORDSET) # make it to watch the Redis Set data structure.
                    pipe.multi()
                    stream1_id_list = [] # list which contains id's which we need to acknowledge for stream1.(w_0)
                    stream2_id_list = [] # list which contains id's which we need to acknowledge for stream2.(w_1)
                    for stream in res:  # [['w_0', [{'id', word}]], [], []]
                        stream_id = stream[0]
                        for item in stream[1]:
                            msg_id = item[0]
                            for word, amount in item[1].items():
                                pipe.zadd(WORDSET, {word: amount}, incr=True)   # used zadd and passed {word: amount}, setting flag incr=True
                            if stream_id == stream1:
                                stream1_id_list.append(msg_id)
                            else:
                                stream2_id_list.append(msg_id)

                    if len(stream1_id_list):    
                        pipe.xack(stream1, groupname, *stream1_id_list)
                    if len(stream2_id_list):
                        pipe.xack(stream2, groupname, *stream2_id_list)
                    pipe.execute()
                    break
                except redis.WatchError:
                    pipe.unwatch()
                    pass
        