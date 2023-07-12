import sys
from time import sleep

from pexpect import pxssh

from myrds import Lab3Redis, ConsistentRedis, AvailableRedis
import subprocess as sp
import os

IPS = ["10.17.51.22", "10.17.51.250", "10.17.10.18"]

def get_redis(consistent: bool) -> Lab3Redis:
  if consistent:
    return ConsistentRedis(IPS)
  return AvailableRedis(IPS)


def get_full_name(ip: str) -> str:
  return f'student@{ip}'


def setup_rabbit(ip: str) -> None:
  print(f"\n\t === Starting rabbitmq on {ip}")
  s = pxssh.pxssh()
  s.login(ip, 'rabbitmq', 'rabbitmq')
 
  s.sendline('rabbitmqctl stop_app')
  s.prompt()
  print(s.before)
  print("Rabbitmq Stopped")

  sleep(5)  # Wait for rabbitmq to stop

  s.sendline('rabbitmqctl start_app')
  s.prompt()
  print(s.before)
  print("Rabbitmq Started")

  s.sendline('rabbitmqctl list_users')
  s.prompt()
  list_users = (s.before).decode()
  print("Rabbitmq Stopped")

  if 'test' not in list_users:
    s.sendline('rabbitmqctl add_user test test')
    s.prompt()
    print(s.before)
    print("Added Test User")

    s.sendline('rabbitmqctl set_permissions -p / test \'.*\' \'.*\' \'.*\'')
    s.prompt()
    list_users = s.before
    print("Permissions are set")

  s.logout()


def copy_code(ip: str) -> None:
  print(f"=== Copying code to {ip}")
  full_name = get_full_name(ip)

  # Create the folders and copy all the files
  print("Creating lab3 folder")
  sp.run(['ssh', full_name, 'mkdir -p ~/labs/lab3/']).check_returncode()
  sp.run(['ssh', full_name, 'rm -rf ~/labs/lab3/*']).check_returncode()

  print("Copying code")
  mydir = os.path.dirname(os.path.realpath(__file__))
  sp.run(['scp', '-r', mydir, f'{full_name}:~/labs/']).check_returncode()


def setup_rds(ip: str, consistent: bool) -> None:
  '''We expect you to have redis.conf and '''
  print(f"=== Setup consistent={consistent} redis on {ip}")
  s = pxssh.pxssh()
  s.login(ip, 'student', 'guess123') #Update the password
  s.prompt()

  s.sendline(f'redis-cli -h {ip} SHUTDOWN')
  s.prompt()
  print(s.before)
  print("Redis shutdown")

  if consistent:
    s.sendline('redis-server redis.conf --loadmodule ~/redisraft/redisraft.so')

    if ip == IPS[0]:
      s.sendline(f'redis-cli -h {ip} RAFT.CLUSTER INIT')
      s.prompt()
    else:
      s.sendline(f'redis-cli -h {ip} RAFT.CLUSTER JOIN {IPS[0]}')
      s.prompt()
  else:
    s.sendline('nohup redis-server redis.conf &')
    s.prompt()
    s.sendline(f'redis-cli -h {ip} flushall')
    s.prompt()
  print("Redis restarted")

  s.logout()


def setup_celery(ip: str, worker_name: str) -> None:
  print(f"=== Setup celery workers on {ip}")
  s = pxssh.pxssh()
  s.login(ip, 'student', 'guess123') # Update the password
  # s.sendline('redis-server redis.conf --loadmodule ~/redisraft/redisraft.so')
  s.sendline('cd ~/labs/lab3/')
  s.prompt()
  print(s.before)
  print("Inside the lab3 folder")

  s.sendline('kill -9 $(ps -ef | grep celery | awk \'{print $2}\')')
  s.prompt()
  print(s.before)
  print("Celery Tasks deleted")

  s.sendline('celery -A tasks purge -f')
  s.prompt()
  print(s.before)
  print("Purge celery task")

  s.sendline(
    f'nohup celery -A tasks worker --loglevel=INFO --concurrency=8 -n {worker_name}@%h &')
  s.prompt()
  print(s.before)
  print("Celery Worker started")

  s.logout()


if __name__ == '__main__':
  # Invoke this file as `python3 config.py True` to run the consistent version.
  CONSISTENT = False
  if sys.argv[1].lower() == 'true':
    CONSISTENT = bool(sys.argv[1])

  print(f" === Configuring for consistent = {CONSISTENT} ===")

  setup_rabbit(IPS[0])

  for ip in IPS:
    copy_code(ip)
    setup_rds(ip, CONSISTENT)

  for i, ip in enumerate(IPS):
    worker_name = f'worker{i}'
    setup_celery(ip, worker_name)
