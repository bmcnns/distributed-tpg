# tasks.py
import subprocess
import numpy as np
from celery import Celery, group
from multiprocessing import Pool, cpu_count

app = Celery('tasks', broker='amqp://guest:guest@192.168.4.122:5672//', backend='rpc')

@app.task
def start_simulator_runner(worker_id, batch_size, teams, generation):
    print(f"Starting simulator runner for worker {worker_id} with teams {teams}")
    command = f"python simulator_runner.py --worker-id {worker_id} --batch-size {batch_size} --teams {' '.join(teams)} --generation {generation}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.stdout 

def start_simulator_runners(worker_batch_sizes, teams_per_worker, generation):
    tasks = []

    for worker_id, batch_size in worker_batch_sizes.items():
        teams = teams_per_worker.get(worker_id, [])
        result = start_simulator_runner.delay(worker_id, batch_size, teams, generation).get()
        results.append(result)

    return results
