import sys, time
import os
from celery import chord, group
import tasks
import redis

'''-----------------------------------------------------

Redis is an in-memory data structure store
used as a database, cache, message broker
and streamline engine

Redis provides data structures such as string, hashes
lists, set and sorted set with range query

Redis has built-in replication and provides high-availablity

Atomic operation on data-structures are allowed

Mostly performance is because of in-memory storage

It supports async replication
Redis is a single threaded, so commands are executed sync.

redis.conf (to change config)
'''

'''-----------------------------------------------------
Celery(open source) is a task queue,
Goal of celery : simple, flexible, and reliable distributed system
'''



'''-----------------------------------------------------
redis-cli, the Redis command line interface

The redis-cli is a terminal program used to send commands to 
and read replies from the Redis server


'''


r = redis.Redis(port = '6579') # Redis server is listening on port 6579
r.flushall() # Flush all existing key in the database. O(N) : N is number of keys
r.bgsave(False) # Don't save redis database in background.

if (len(sys.argv) < 2):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]


def reduce_and_print(ls):
    tree = [{} for i in range(0, 2 * len(ls))]
    tree[len(ls):] = ls
    for i in range(len(ls) - 1, -1, -1):
        left = 2 * i
        right = 2 * i + 1
        dict = tree[right] # can be more fast ...
        for k1, v1 in tree[left].items():
            if k1 in dict:
                dict[k1] = dict[k1] + v1
            else:
                dict[k1] = v1
        tree[i] = dict
    return tree[0]


# Read all the files in the directory
abs_files=[os.path.join(pth, f) for pth, dirs, files in os.walk(DIR) for f in files]
# Decide the number of chunks (To break the files)
no_of_chunks = 16 
# Compute the each chunk size, based on the total number of files.
chunk_size = (len(abs_files) + no_of_chunks - 1) // no_of_chunks 
# Create and list of lists, 
# It contians no_of_chunks list, where each contain names of the files with in the chunks.
res = group(tasks.map_files.s(abs_files[idx: idx + chunk_size]) for idx in range(0, len(abs_files), chunk_size))().get()

ans = reduce_and_print(res) # small linear merge, to avoid sending huge data to redis again.
print(ans) # printing result 


''' 

Celery Topics:

Chain:
    -@app.task define the task for celery.
    @app.task
    def add(x, y):
        return x + y


    -When we chain task, then second task takes the input from the first task.
    
    from celery import chain
    res = chain(add.s(1, 2), add.s(3)).apply_async() 

    -Chaining is also done using | 
    res2 = (add.s(1, 2) | add.s(3)).apply_async()

Group:
    Groups are used to execute taks in parallel.

    job = group([
        add.s(2, 2),
        add.s(4, 4)
    ])

    result = job.apply_async()

Chord 

    Chord has two parts
        - chord(header)(callback)

        Header is simly a "GROUP" of tasks and callback is called once the the
        group or tasks are finished.

        ex.
        callback = tsum.s()
        header = [add.s(3, 2), add.s(5, 4)]
        result = chord(header)(callback)
        result.get()


apply_async() : returns as AsyncResult object immediately.
once you call get() then you block.

'''