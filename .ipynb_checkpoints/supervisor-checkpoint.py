import time
from datetime import datetime

from tasks import start_workers
from tpg.model import Model
from parameters import Parameters
from db.database import Database
import duckdb
import pandas as pd
import pprint

from tpg.mutator import Mutator

if __name__ == '__main__':

    Database.connect(
        user="postgres",
        password="template!PWD",
        host="192.168.4.196",
        port=5432,
        database="postgres"
    )

    Database.clear()
    Database.load()
    Database.connect_duckdb()

    model = Model()

    # Update the database
    for program in model.programPopulation:
        Database.add_program(program)
    for team in model.teamPopulation:
        Database.add_team(team)


    time_elapsed = 0.0
    benchmarking_data = []
    for generation in range(1, 10+1):
        start_time = datetime.now()

        print(f"Starting generation {generation}...")

        teams_per_worker = {
            "desktop": [str(id) for id in Database.get_root_teams()]
        }

        # Collect training information
        start_workers(
            teams_per_worker,
            generation,
            model
        )

        for team in model.teamPopulation:
            if team.id not in model.get_survivor_ids(generation):
                for program in team.programs:
                    Database.remove_program(program)
                Database.remove_team(team)

        model.repopulate()

                print(Database.get_ranked_teams(generation).sort_values('rank').head(10))

        end_time = datetime.now()
        delta_time = end_time - start_time
        time_elapsed += delta_time.total_seconds()
        benchmarking_data.append([generation,time_elapsed])

    df = pd.DataFrame(benchmarking_data, columns=['generation', 'time_elapsed'])
    df.to_csv("car_racing_12_envs.csv")


