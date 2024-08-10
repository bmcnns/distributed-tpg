import random
import numpy as np
from celery import Celery, group
import multiprocessing
import time
import gymnasium
import pandas as pd
import psutil

from db.database import Database
from parameters import Parameters

app = Celery('tasks', broker=f'amqp://guest:guest@{Parameters.DATABASE_IP}:5672//', backend='rpc')

app.conf.update(
    task_serializer='pickle',
    result_serializer='pickle',
    accept_content=['pickle']
)


def record_cpu_utilization(pids, worker_name, run_id, interval=1):
    data = []

    while True:
        if not any(psutil.pid_exists(pid) for pid in pids):
            print("All processes have stopped")
            break

        current_time = time.time()
        cpu_perc = psutil.cpu_percent(percpu=True, interval=None)
        for i, percent in enumerate(cpu_perc):
            row = {
                "run_id": run_id,
                "time": current_time,
                "worker": worker_name,
                "core": i,
                "utilization": percent
            }

            data.append(row)

        time.sleep(interval)  # Ensure consistent intervals

    Database.connect(
        user="postgres",
        password="template!PWD",
        host=Parameters.DATABASE_IP,
        port=5432,
        database="postgres")

    Database.add_cpu_utilization_data(data)

    Database.disconnect()


def run_environment(generation, team_id, model, seed, run_id, shared_list):
    env = gymnasium.make("CartPole-v1")

    np.random.seed(seed)
    random.seed(seed)

    obs = env.reset(seed=seed)[0]
    team = model.get_team(team_id)

    step = 0

    training_data = []
    while step < Parameters.MAX_NUM_STEPS:
        #state = obs.flatten()
        state = obs
        action = Parameters.ACTIONS.index(team.getAction(model.teamPopulation, state, visited=[]))
        obs, rew, term, trunc, info = env.step(action)

        #Database.add_observation(run_id, time.time(), obs)

        step += 1

        training_data.append({
            "run_id": run_id,
            "generation": generation,
            "team_id": team_id,
            "action": action,
            "reward": rew,
            "is_finished": term or trunc,
            "time_step": step,
            "time": time.time()
        })

        if term or trunc:
            break

    env.close()

    shared_list.extend(training_data)

@app.task()
def start_worker(generation, teams, model, worker_name, seed, run_id, batch_size):
    processes = []

    manager = multiprocessing.Manager()
    training_data = manager.list()

    num_batches = len(teams) // batch_size
    batches = np.array_split(teams, num_batches)

    for batch in batches:
        for team_id in batch:
            process = multiprocessing.Process(target=run_environment, args=(generation, team_id, model, seed, run_id, training_data))
            processes.append(process)
            process.start()

        pids = [process.pid for process in processes]
        #benchmarker = multiprocessing.Process(target=record_cpu_utilization, args=(pids, worker_name, run_id))
        #benchmarker.start()

        # When all teams are finished, the information is sent back to the supervisor
        for process in processes:
            process.join()

    Database.connect("postgres", "template!PWD", Parameters.DATABASE_IP, 5432, "postgres")
    Database.add_training_data(training_data)
    Database.disconnect()

    print("Finished adding the training data to the database.")

    # Wait for the benchmarker to finish (should be done once any of the processes are done)
    #benchmarker.join()

    print("All workers finished.", len(processes))

def start_workers(teams_per_worker, worker_batch_sizes, generation, model, seed, run_id):
    tasks = []

    for worker_id, teams in teams_per_worker.items():
        batch_size = worker_batch_sizes.get(worker_id)
        task = start_worker.s(generation, teams_per_worker, model, worker_id, seed, run_id, batch_size).set(queue=f'{worker_id}')
        tasks.append(task)

    result = group(tasks)()
    result.get()
