import time
import random
from tasks import start_workers
from tpg.model import Model
from parameters import Parameters
from db.database import Database
import duckdb
import pandas as pd
import pprint
import uuid

from tpg.mutator import Mutator


def get_teams_assigned_to_worker(run_id, configuration):
    root_teams = [str(team_id) for team_id in Database.get_root_teams(run_id)]
    counter = 0
    assigned_teams = {}

    for worker_name, team_count in configuration['team_distribution']:
        assigned_teams[worker_name] = root_teams[counter:counter + team_count]
        counter += team_count

    return assigned_teams


def train(run_id, configuration, num_generations):
    model = Model()

    # Update the database
    for team in model.teamPopulation:
        Database.add_team(run_id, team)

        for program in team.programs:
            Database.add_program(run_id, program, team)

    time_elapsed = 0.0
    benchmarking_data = []

    seeds = [random.randint(0, 2 ** 31 - 1) for _ in range(num_generations)]

    for generation in range(1, num_generations + 1):
        print(f"Starting generation {generation}...")

        # Collect training information
        start_workers(
            get_teams_assigned_to_worker(run_id, configuration),
            configuration['batch_sizes'],
            generation,
            model,
            seeds[generation - 1],
            run_id
        )

        teams_to_update_lucky_breaks = []
        programs_to_remove = []
        teams_to_remove = []

        print("Updating teams and programs now")

        root_teams = Database.get_root_teams(run_id)
        for team in model.teamPopulation:
            # We only purge root teams, otherwise, there would never be any surviving child teams.
            if str(team.id) not in [str(team_id) for team_id in root_teams]:
                continue
            if team.id not in model.get_survivor_ids(run_id, generation):
                if team.luckyBreaks > 0:
                    team.luckyBreaks -= 1
                    teams_to_update_lucky_breaks += team
                    Database.update_team(run_id, team, team.luckyBreaks)
                    continue

                for program in team.programs:
                    programs_to_remove += (program.id, team.id)

                teams_to_remove += team.id

                for _team in model.teamPopulation:
                    if str(team.id) == _team.id:
                        model.teamPopulation.remove(team)

        Database.remove_programs(run_id, programs_to_remove)
        Database.remove_teams(run_id, teams_to_remove)

        print("Cloning existing teams and adding new teams to the database now")
        model.repopulate(run_id, generation)

        print("Updating lucky breaks now")
        # Mark the lucky breaks now
        n = 1
        top_n_teams = Database.get_ranked_teams(run_id, generation).sort_values('rank').head(n)['team_id'].to_list()
        for team in model.teamPopulation:
            if str(team.id) in [str(team_id) for team_id in top_n_teams]:
                team.luckyBreaks += 1
                Database.update_team(run_id, team, team.luckyBreaks)

        print("Showing output now")
        print(Database.get_ranked_teams(run_id, generation).sort_values('rank').head(10))

        print("Adding time monitor data now")
        Database.add_time_monitor_data(run_id, generation, time.time())

        print(f"Finished generation {generation}...")

        model.save(f"./saved_models/lunar_lander_{generation}.pkl")


if __name__ == '__main__':
    print("Connecting to the database...")

    Database.connect(
        user="postgres",
        password="template!PWD",
        host=Parameters.DATABASE_IP,
        port=5432,
        database="postgres",
    )

    print("Database connected.")

    num_runs = 1
    configurations = [
        {
            "team_distribution": [("alice", 360)],
            "batch_sizes": {"alice": 1}
        }
    ]

    assert num_runs == len(configurations), "A configuration must be given for each run"
    run_ids = [uuid.uuid4() for _ in range(num_runs)]

    for run_id, configuration in zip(run_ids, configurations):

        Database.add_compute_config(
            run_id,
            str(configuration["team_distribution"]).replace("'", '"'),
            str(configuration["batch_sizes"]).replace("'", '"')
        )

        print(f"RUN_ID: {run_id}")
        train(run_id=run_id, configuration=configuration, num_generations=10)
