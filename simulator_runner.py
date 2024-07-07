import argparse
import envpool
import numpy as np
import pandas as pd
import duckdb
from db.database import Database
import uuid
import random
import sys

def run_simulators(worker_id, batch_size, num_envs, teams, generation):

    env = envpool.make(
        "LunarLander-v2",
        env_type="gym",
        num_envs=num_envs,
        batch_size=batch_size,
        seed=random.randint(0, 2147483647))

    env.async_reset()
    num_finished_teams = 0

    # A dictionary tracking the latest timestep for each team
    timestep_record = { team: 0 for team in teams }

    cumulative_df = pd.DataFrame(columns=['generation', 'environment_id', 'team_id', 'is_terminated', 'is_truncated', 'reward', 'time_step', 'action'])

    while (num_finished_teams < num_envs):
        obs, rew, term, trunc, info = env.recv()

        num_finished_teams += term.sum() + trunc.sum()

        env_id = info['env_id']

        teams_in_batch = np.array(teams)[env_id]
        
        time_steps = [timestep_record[team] for team in teams_in_batch]
        
        action = np.random.randint(0, 4, size=batch_size, dtype='int32')

        new_data = {
            'generation': generation,
            'environment_id': env_id,
            'team_id': teams_in_batch,
            'is_terminated': term,
            'is_truncated': trunc,
            'reward': rew,
            'time_step': time_steps,
            'action': action
        }

        new_df = pd.DataFrame(new_data)
        cumulative_df = pd.concat([cumulative_df, new_df], ignore_index=True)

        for team in teams_in_batch:
            timestep_record[team] += 1

        env.send(action, env_id)

    transformed_df = duckdb.query("""
        WITH team_finished_timesteps AS (
            SELECT team_id, 
            MIN(time_step) AS last_time_step 
            FROM cumulative_df 
            WHERE is_terminated = true OR is_truncated = true
            GROUP BY team_id
        )

        SELECT generation,
                    environment_id, 
                    team_id,
                    is_terminated,
                    is_truncated,
                    reward,
                    time_step,
                    action 
        FROM team_finished_timesteps
        JOIN cumulative_df USING (team_id)
        WHERE time_step <= last_time_step
    """).df()

    # Drop data that has been auto-reset.
    Database.connect(
        user="postgres",
        password="template!PWD",
        host="192.168.4.122",
        port=5432,
        database="postgres"
    )
    
    Database.load()

    Database.store("training", df=transformed_df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Simulator Runner")
    parser.add_argument("--worker-id", type=str, help="ID of the worker running the task")
    parser.add_argument("--batch-size", type=int, help="Batch size (number of cores available)")
    parser.add_argument("--num-envs", type=int, help="Total number of simulations to run")
    parser.add_argument("--teams", type=str, help="The IDs of the teams being used by this worker", nargs='+')
    parser.add_argument("--generation", type=int, help="The generation that this simulator run is part of")
    args = parser.parse_args()

    run_simulators(args.worker_id, args.batch_size, args.num_envs, args.teams, args.generation)
