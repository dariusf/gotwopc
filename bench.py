#!/usr/bin/env python

import subprocess
import pathlib
import os
import time
import shutil

def mkdirp(path):
  pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def start_master(f, n):
  return subprocess.Popen(['./main', '-m', '-n', str(n)],
    stdout=f,
    stderr=subprocess.STDOUT,
    cwd=os.getcwd())

def start_replica(f, n):
  return subprocess.Popen(['./main', '-r', '-i', str(n)],
    stdout=f,
    stderr=subprocess.STDOUT,
    cwd=os.getcwd())

def start_client(f):
  return subprocess.Popen(['./main'],
    stdout=f,
    stderr=subprocess.STDOUT,
    env={'CLIENT': '1'},
    cwd=os.getcwd())

if __name__ == "__main__":
  replica_count = 2
  
  shutil.rmtree('out', ignore_errors=True)
  shutil.rmtree('data', ignore_errors=True)
  shutil.rmtree('logs', ignore_errors=True)
  mkdirp('out')
  subprocess.check_call(['go', 'build', 'main.go'])

  replica_files = []
  replica_processes = []
  with open('out/master.log', 'w') as master_f, open('out/client.log', 'w') as client_f:
    master_process = start_master(master_f, replica_count)
    for i in range(replica_count):
      fr = open(f'out/replica{i}.log', 'w')
      replica_files.append(fr)
      replica_processes.append(start_replica(fr, i))

    print('waiting for processes to start')
    time.sleep(3)
    start_client(client_f).wait()
    print('client terminated')

    for f in replica_files:
      f.close()
    for p in replica_processes:
      p.terminate()
      p.wait()

    master_process.terminate()
    master_process.wait()
  print('done')
