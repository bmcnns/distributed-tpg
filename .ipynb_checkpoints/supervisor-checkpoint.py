import time
from tasks import start_workers
from tpg.model import Model
from parameters import Parameters
from db.database import Database
import duckdb
import pandas as pd
import pprint

from tpg.mutator import Mutator

if __name__ == '__main__':

    print("Connecting to the database...")

    Database.connect(
        user="postgres",
        password="template!PWD",
        host=Parameters.DATABASE_IP,
        port=5432,
        database="postgres"
    )

    print("Database connected.")

    Database.clear()
    Database.load()
    Database.connect_duckdb(Parameters.DATABASE_IP)

    print("Duckdb connected.")

    model = Model()

    # Update the database
    for team in model.teamPopulation:
        Database.add_team(team)

        for program in team.programs:
            Database.add_program(program, team)

    time_elapsed = 0.0
    benchmarking_data = []
    for generation in range(1, 200+1):
        print(f"Starting generation {generation}...")

        teams_per_worker = {
            "desktop": [str(id) for id in Database.get_root_teams()]
      #      "raspberrypi": [str(id) for id in Database.get_root_teams()[240:300]],
     #       "beelink": [str(id) for id in Database.get_root_teams()[300:360]],
        }

        worker_batch_sizes = {
            "desktop": 12,
      #      "raspberrypi": 4,
      #      "beelink": 4,
      #      "macbook": 8
        }

        # Collect training information
        start_workers(
            teams_per_worker,
            worker_batch_sizes,
            generation,
            model
        )

        for team in model.teamPopulation:
            # We only purge root teams, otherwise, there would never be any surviving child teams.
            if team.id not in [str(team_id) for team_id in Database.get_root_teams()]:
                print("Leaf team detected. Not going to delete this one.")
                continue
            if team.id not in model.get_survivor_ids(generation):
                if team.luckyBreaks > 0:
                    team.luckyBreaks -= 1
                    Database.update_team(team, team.luckyBreaks)
                    continue

                for program in team.programs:
                    Database.remove_program(program, team)
                Database.remove_team(team)

                for _team in model.teamPopulation:
                    if str(team.id) == _team.id:
                        model.teamPopulation.remove(team)
                        print(f"Removing team {team.id}")

        model.repopulate()

        # Mark the lucky breaks now
        n = 3
        top_n_teams = Database.get_ranked_teams(generation).sort_values('rank').head(3)['team_id'].to_list()
        for team in model.teamPopulation:
            if str(team.id) in [ str(team_id) for team_id in top_n_teams ]:
                team.luckyBreaks += 1
                Database.update_team(team, team.luckyBreaks)


        print(Database.get_ranked_teams(generation).sort_values('rank').head(10))

        benchmarking_data = pd.DataFrame([{
            "generation": generation,
            "time": time.time()
        }])

        Database.store("time_monitor", benchmarking_data)
        print(f"Finished generation {generation}...")
