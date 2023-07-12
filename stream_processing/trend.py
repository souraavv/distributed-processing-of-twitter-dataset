import sys
import os
from operator import itemgetter

if (len(sys.argv) < 2):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]

abs_files=[os.path.join(pth, f) for pth, dirs, files in os.walk(DIR) for f in files]

wc = {}

for filename in abs_files:
    with open(filename, mode='r') as f:
        for text in f:
            if text == '\n':
                continue
            sp = text.split(',')[4:-2]
            tweet = " ".join(sp)
            for word in tweet.split(" "):
                word = word.lower()
                if word not in wc:
                    wc[word] = 0
                wc[word] = wc[word] + 1

res = dict(sorted(wc.items(), key = itemgetter(1), reverse = True)[:10])

# printing result
print("The top 10 value pairs are  " + str(res))
