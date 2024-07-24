import argparse
import envpool
import numpy as np
import pandas as pd
import duckdb
from db.database import Database
import uuid
import random
import sys
import time

from tpg.model import Model
from parameters import Parameters

def run_simulators(worker_id, batch_size, num_envs, assigned_teams, generation, model_file_path):

	model = Model.load(model_file_path)

	teams = np.array([ team for team in model.teamPopulation if team.id in assigned_teams ])
	if len(teams) == 0:
		print(f"No teams found. {assigned_teams}")
	
	env = envpool.make(
		"LunarLander-v2",
		env_type="gym",
		num_envs=num_envs,
		batch_size=batch_size,
		seed=random.randint(0, 2147483647)
	)

	
	env.async_reset()
	num_finished_teams = 0

	# A dictionary tracking the latest timestep for each team
	timestep_record = { team: 0 for team in teams }

	finished_teams = { team: False for team in assigned_teams }

	cumulative_df = pd.DataFrame(columns=['generation', 'environment_id', 'team_id', 'is_terminated', 'is_truncated', 'reward', 'time_step', 'action'])

	# While not all environments are finished
	while not all(finished_teams.values()) == True:
		print(finished_teams)
		# print(f"received team count: {len(model.teamPopulation)}")
		# print(len(assigned_teams))
		obs, rew, term, trunc, info = env.recv()

		df = pd.DataFrame(obs, columns=[f'obs{i+1}' for i in range(8)])
		df['istruncated'] = trunc
		df['isterminated'] = term
		
		num_finished_teams += term.sum() + trunc.sum()

		env_id = info['env_id']

		df['team_id'] = [ team.id for team in teams ]

		finished_team_ids = df[(df['isterminated'] == True ) | (df['isterminated'] == True)]['team_id'].tolist()

		for team_id in finished_team_ids:
			finished_teams[team_id] = True
			
							   
		print(df.head())

		# print(env_id)

		str_actions = []
		int_actions = []
		
		for team, observation in zip(teams[env_id], obs):
			# print(f"{team.id} has received observation {observation}")
			action = team.getAction(teamPopulation=model.teamPopulation, state=observation, visited=[])
			# print(f"team {team.id} has chosen action {action}")
			int_action = Parameters.ACTIONS.index(action)
			str_actions.append(action)
			int_actions.append(int_action)

		str_actions = np.array(str_actions)
		int_actions = np.array(int_actions)
		#print(str_actions)
		#print(int_actions)
		
		time_steps = [timestep_record[team] for team in teams[env_id]]

		#print(env_id)
		
		new_data = {
			'generation': generation,
			'environment_id': env_id,
			'team_id': [team.id for team in teams[env_id]],
			'is_terminated': term,
			'is_truncated': trunc,
			'reward': rew,
			'time_step': time_steps,
			'action': int_actions
		}

		new_df = pd.DataFrame(new_data)

		if cumulative_df.empty:
			cumulative_df = new_df.copy()
		else:
			cumulative_df = pd.concat([cumulative_df, new_df], ignore_index=True)

		#print(cumulative_df.head())

		for team in teams[env_id]:
			timestep_record[team] += 1

		env.send(int_actions, env_id)

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
	parser.add_argument("--model-file-path", type=str, help="The path to the Pickle file holding our TPG model")
	args = parser.parse_args()

	run_simulators(args.worker_id, args.batch_size, args.num_envs, args.teams, args.generation, args.model_file_path)
