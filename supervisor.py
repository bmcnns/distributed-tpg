from tasks import start_simulator_runners
from datetime import datetime
import pandas as pd
from tpg.model import Model
from tpg.team import Team
from tpg.mutator import Mutator
import uuid
from db.database import Database
import duckdb
import os
import time

from parameters import Parameters


if __name__ == '__main__':

	Database.connect(
		user="postgres",
		password="template!PWD",
		host="192.168.4.122",
		port=5432,
		database="postgres"
	)

	Database.clear()
	Database.load()
	Database.connect_duckdb()

	model = Model()

	for team in model.teamPopulation:
		Database.add_team(team)

	for program in model.programPopulation:
		Database.add_program(program)

	for team in model.teamPopulation:
		for program in team.programs:
			Database.update_program_team_id(program, team)

	print(len(Database.get_root_teams()))
	
	worker_batch_sizes = {
		'desktop': 4,
	}

	worker_num_envs = {
		'desktop': 4,
	}

	num_generations = 100

	time_elapsed = 0
	data = []

	for generation in range(num_generations):
		print(f"Starting generation {generation}...")

		start_time = datetime.now()

		print(f"team population: {len(model.teamPopulation)}")
		model_file_path = "temp/tpg.pkl"
		model.save(model_file_path)

		teams_per_worker = {
				'desktop':  [str(team) for team in Database.get_root_teams()],
		}

		start_simulator_runners(
		    worker_batch_sizes,
		    worker_num_envs,
		    teams_per_worker,
		    generation=generation,
			model_file_path=model_file_path
		)

		if os.path.exists(model_file_path):
			os.remove(model_file_path)
		else:
			raise Exception("Error when deleting the temporary model pickle file. Can't find the temp file.")

		print("Starting natural selection...")

		numTeamsKept = Parameters.POPULATION_SIZE // 2

		selected_team_ids = Database.get_ranked_teams().df()['team_id'][:numTeamsKept].to_list()
		selected_team_ids = [ str(team_id) for team_id in selected_team_ids ]
		
		root_teams = [str(team) for team in Database.get_root_teams()]

		for team in model.teamPopulation:
			print("local teams: ", len(root_teams))
			if str(team.id) not in root_teams:
				continue
			if team.id in selected_team_ids:
				copied_team = team.copy()
				Mutator.mutateTeam(model.programPopulation, model.teamPopulation, copied_team)
				model.teamPopulation.append(copied_team)
				Database.add_team(copied_team)
			else:
				model.teamPopulation.remove(team)
				Database.remove_team(team)
			
		time.sleep(10)

		print("Teams removed from database. Showing teams now")
		
		print(Database.get_teams())

		print("Finished natural selection...")

		print(f"Finished generation {generation}...")

		end_time = datetime.now()

		dt = end_time - start_time

		time_elapsed += dt.total_seconds()

		data.append( [generation,  time_elapsed] )

	benchmarking_data = pd.DataFrame(data, columns=['Generation', 'Time Elapsed'])
	benchmarking_data.to_csv("generation_data/distributed_benchmarking_by_generation.csv")
