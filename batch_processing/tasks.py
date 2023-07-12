from celery import Celery, chord, group
app = Celery('tasks', backend='redis://localhost:6579', broker='pyamqp://localhost')


'''
Celery::task Function : linear_meger()
Param(ls:Dictionary) : containing the {word: freq}
Used tree kind of hierarchy to merge finally to a single node
- Last n nodes are kept with the original data
- Similar to merging work in merge sort
'''

@app.task(acks_late=True)
def linear_merger(key_value_dict):
    merge_tree = [{} for i in range(0, 2 * len(key_value_dict))] # Similar merge_tree as merge sort do.
    merge_tree[len(key_value_dict):] = key_value_dict # setup already known value in the leaf.
    for i in range(len(key_value_dict) - 1, -1, -1):
        left_child = 2 * i # Left Child
        right_child = 2 * i + 1 # Right child
        cur_node = merge_tree[right_child] # Put right_child values directly
        for word, freq in merge_tree[left_child].items(): # Iterate on the left_child values.
            if word in cur_node: # If the key is already present, then add
                cur_node[word] = cur_node[word] + freq
            else: # If it is fresh key, then update.
                cur_node[word] = freq
        merge_tree[i] = cur_node # Finally update parent.
    return merge_tree[0]

'''
Celery::task map_files(cur_chunk_file:list["string"])
It takes file name, first pre-process them from the function
add_word, they return the a dictionary which contains {word:freq}
'''
@app.task(acks_late=True)
def map_files(cur_chunk_files):
    return linear_merger([add_word(file) for file in cur_chunk_files])
    
'''
add_words(filename:"string")
This function is provided a file, it converts that to a dictionary
{word: freq}
'''
def add_word(filename):
    word_count = {}
    with open(filename, mode='r', newline='\r') as f:
        for text in f:
            if text == '\n':
                continue
            sp = text.split(',')[4:-2] # fetch the text from the tweet.
            tweet = " ".join(sp)
            for word in tweet.split(" "):
                if word not in word_count:
                    word_count[word] = 0
                word_count[word] = word_count[word] + 1
    return word_count

