import numpy as np
from celery import Celery, group
import multiprocessing
import time
import gym
import pandas as pd
import duckdb

from db.database import Database
from parameters import Parameters

app = Celery('tasks', broker='amqp://guest:guest@192.168.4.196:5672//', backend='rpc')

app.conf.update(
    task_serializer='pickle',
    result_serializer='pickle',
    accept_content=['pickle']
)

def run_environment(generation, team_id, model):
    Database.connect(
        user="postgres",
        password="template!PWD",
        host="192.168.4.196",
        port=5432,
        database="postgres"
    )

    env = gym.make("CarRacing-v1", continuous=False)

    obs = env.reset()
    finished = False

    team = model.get_team(team_id)

    data = []
    step = 0
    while not finished and step < Parameters.MAX_NUM_STEPS:
        action = Parameters.ACTIONS.index(team.getAction(model.teamPopulation, obs.flatten(), visited=[]))
        obs, rew, finished, info = env.step(action)

        
        
        data.append({
            "generation": generation,
            "team_id": team_id,
            "action": action,
            "reward": rew,
            "is_finished": finished,
            "time_step": step
        })

        step += 1

    df = pd.DataFrame(data, columns=['generation', 'team_id', 'action', 'reward', 'is_finished', 'time_step'])
    Database.store("training", df)

    env.close()


@app.task()
def start_worker(generation, teams, model):
    processes = []

    for team_id in teams:
        process = multiprocessing.Process(target=run_environment, args=(generation, team_id, model))
        process.start()
        processes.append(process)

    # When all teams are finished, the information is sent back to the supervisor
    for process in processes:
        process.join()

    print("All workers finished.", len(processes))


def start_workers(teams_per_worker, generation, model):
    tasks = []

    for worker_id, teams in teams_per_worker.items():

        num_batches = len(teams) // Parameters.NUM_ENVS
        batches = np.array_split(teams, num_batches)

        for batch in batches:
            task = start_worker.s(generation, batch, model).set(queue=f'{worker_id}')
            tasks.append(task)

    result = group(tasks)()
    result.get()
