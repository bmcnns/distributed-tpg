# tasks.py
import subprocess
from celery import Celery, group
import numpy as np

app = Celery('tasks', broker='amqp://guest:guest@localhost:5672//', backend='rpc')

@app.task()
def start_simulator_runner(worker_id, batch_size, num_envs, teams, generation):
    print("starting simulator runner")
    command = f"python simulator_runner.py --worker-id {worker_id} --batch-size {batch_size} --num-envs {num_envs} --teams {' '.join(teams)} --generation {generation}"
    subprocess.run(command, shell=True)
    
def start_simulator_runners(worker_batch_sizes, num_simulators_per_worker, worker_num_envs, teams_per_worker, generation):
    tasks = []
    async_results = []
    
    for worker_id, batch_size in worker_batch_sizes.items():
        num_simulators = num_simulators_per_worker.get(worker_id, 0)
        num_envs = worker_num_envs.get(worker_id, 0)
        teams = teams_per_worker.get(worker_id, 0)
        
        if num_simulators > 0 and num_envs > 0:
            num_tasks = num_simulators // num_envs

            assigned_teams = np.array_split(teams, num_tasks)

            for task_num in range(num_tasks):
                teams_sent_to_worker = list(assigned_teams[task_num])

                task = start_simulator_runner.s(worker_id, batch_size, num_envs, teams_sent_to_worker, generation).set(queue=f'{worker_id}')

                tasks.append(task)

                async_result = task.apply_async()
                async_results.append(async_result)

            # Wait for results
            for async_result in async_results:
                async_result.get()
