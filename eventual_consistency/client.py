import os
import sys
from tasks import mapper
from celery import chord, group
from config import get_redis
from time import sleep, perf_counter
from config import IPS
import subprocess

if __name__ != '__main__':
  sys.exit(1)

# Invoke this file as `python3 client.py True` to run the consistent version.
CONSISTENT=False
if sys.argv[1].lower() == 'true':
  CONSISTENT=bool(sys.argv[1])

print(f"Running with CONSISTENT = {CONSISTENT}")

if (len(sys.argv) < 2):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]
CNT=int(sys.argv[2])

# ======== Network Partition Configuration ==========
# The current node is isolated from other nodes
# Update the current_ip variable with the IP.
current_ip = {'10.17.51.22'}
faulty_node_ips = list(set(IPS).difference(current_ip))

# Firewall Commands
isolate_command = \
  f'''sudo iptables -I INPUT 1 -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -I OUTPUT 1 -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP;'''

heal_command = \
  f'''sudo iptables -D INPUT -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -D OUTPUT -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP;'''

rds = get_redis(CONSISTENT)

abs_files=[os.path.join(pth, f) for pth, _, files in os.walk(DIR) for f in files][:CNT]

size = len(abs_files)
 
print(abs_files)
job = group([mapper.s(filename=file, idx=idx) for idx, file in enumerate(abs_files)])
results=job.apply_async()

# Wait for 2 seconds before isolation starts
# sleep(2)

subprocess.run([isolate_command], shell=True, text=True, input='guess123\n')  # Update with password
print("The network partition is in place")

# Should get inconsistent results, waiting for mapper tasks to complete 
start_time = perf_counter()
print(results.get())
wc = rds.get_top_words(10)
total_time = perf_counter() - start_time
print(wc)
print(f'Total time elapsed before network healing: {total_time}')

subprocess.run([heal_command], shell=True, text=True, input='guess123\n') # Update with password
print("The network partition is healed")

start_time = perf_counter()
wc = rds.get_top_words(10, True)
total_time = perf_counter() - start_time
print(wc)
print(f'Total time elapsed: {total_time}')
