import numpy as np
from celery import Celery, group
import multiprocessing
import time
from datetime import datetime
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

def record_cpu_utilization(pids, interval=0.01):
    data = []
    start_time = datetime.now()

    while True:
        if not any(psutil.pid_exists(pid) for pid in pids):
            print("All processes have stopped")
            break

        current_time = datetime.now()
        delta_time = current_time - start_time
        row = {"time": delta_time.total_seconds()}

        cpu_perc = psutil.cpu_percent(percpu=True, interval=None)
        for i, percent in enumerate(cpu_perc):
            row[f"core_{i}"] = percent

        data.append(row)
        time.sleep(interval)  # Ensure consistent intervals

    df = pd.DataFrame(data)
    df.to_csv("benchmarking/cpu_utilization.csv", index=False)

def run_environment(generation, team_id, model):
    Database.connect(
        user="postgres",
        password="template!PWD",
        host=Parameters.DATABASE_IP,
        port=5432,
        database="postgres"
    )

    env = gymnasium.make("LunarLander-v2")

    obs = env.reset()[0]
    team = model.get_team(team_id)

    data = []
    step = 0
    while step < Parameters.MAX_NUM_STEPS:
        state = obs.flatten()
        action = Parameters.ACTIONS.index(team.getAction(model.teamPopulation, state, visited=[]))
        obs, rew, term, trunc, info = env.step(action)

        step += 1

        data.append({
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

    df = pd.DataFrame(data, columns=['generation', 'team_id', 'action',
                                     'reward', 'is_finished', 'time_step',
                                     'time'])

    Database.store("training", df)

    env.close()


@app.task()
def start_worker(generation, teams, model):
    processes = []

    for team_id in teams:
        process = multiprocessing.Process(target=run_environment, args=(generation, team_id, model))
        process.start()
        processes.append(process)

    pids = [process.pid for process in processes]

    benchmarker = multiprocessing.Process(target=record_cpu_utilization, args=[pids])
    benchmarker.start()

    # When all teams are finished, the information is sent back to the supervisor
    for process in processes:
        process.join()

    # Wait for the benchmarker to finish (should be done once any of the processes are done)
    benchmarker.join()

    print("All workers finished.", len(processes))


def start_workers(teams_per_worker, worker_batch_sizes, generation, model):
    tasks = []

    for worker_id, teams in teams_per_worker.items():
        batch_size = worker_batch_sizes.get(worker_id)
        num_batches = len(teams) // batch_size
        batches = np.array_split(teams, num_batches)

        for batch in batches:
            task = start_worker.s(generation, batch, model).set(queue=f'{worker_id}')
            tasks.append(task)

    result = group(tasks)()
    result.get()
