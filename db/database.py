import numpy as np
import psycopg2 as pg
from psycopg2 import Error
from psycopg2 import sql
import duckdb
from parameters import Parameters


class Database:
    schemas = [
        """
		CREATE TABLE IF NOT EXISTS db.public.training (
			run_id UUID,
			generation INT,
			team_id UUID,
			is_finished BOOLEAN,
			reward FLOAT8,
			time_step INT,
			time FLOAT8,
			action INT,
			PRIMARY KEY (run_id, generation, team_id, time_step)
		)
		""",
        """
			CREATE TABLE IF NOT EXISTS db.public.teams (
                run_id UUID,
                id UUID,
                lucky_breaks INT,
                PRIMARY KEY (run_id, id)
			);
		""",
        """
			CREATE TABLE IF NOT EXISTS db.public.programs (
                run_id UUID,
				id UUID, 
				team_id UUID,
				action VARCHAR,
				pointer UUID,
				CONSTRAINT action_or_pointer_check CHECK (
						(action IS NOT NULL AND pointer IS NULL) OR
						(action IS NULL AND pointer IS NOT NULL)
				),
				PRIMARY KEY (run_id, id, team_id)
			);
		""",
        """
            CREATE TABLE IF NOT EXISTS db.public.cpu_utilization (
                run_id UUID,
                time FLOAT8,
                worker VARCHAR,
                core INT,
                utilization FLOAT8,
                PRIMARY KEY (run_id, time, worker, core)
            );
        """,
        """
			CREATE TABLE IF NOT EXISTS db.public.time_monitor (
				run_id UUID,
				generation INT,
				time FLOAT8,
				PRIMARY KEY (run_id, generation)
			);
		""",
        """
            CREATE TABLE IF NOT EXISTS db.public.observations (
                run_id UUID,
                time FLOAT8,
                observation FLOAT8[]
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS db.public.diversity_cache (
                run_id UUID,
                time FLOAT8,
                team_id UUID,
                profile INT[]
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS db.public.compute_configs (
                run_id UUID PRIMARY KEY,
                team_distribution VARCHAR,
                batch_sizes VARCHAR
            );
        """
    ]

    @classmethod
    def connect(cls, user, password, host, port, database):
        try:
            duckdb.sql("INSTALL postgres;")
            duckdb.sql("LOAD postgres;")
            duckdb.sql(f"ATTACH 'dbname={database} user={user} host={host} password={password}' AS db (TYPE POSTGRES);")

            for schema in cls.schemas:
                duckdb.sql(schema)

        except (Exception, Error) as error:
            print("Error while connecting to database", error)

    @classmethod
    def disconnect(cls):
        duckdb.sql("DETACH db;")

    @classmethod
    def clear(cls):
        duckdb.sql("""
		DROP TABLE IF EXISTS db.public.instructions;
		DROP TABLE IF EXISTS db.public.programs;
		DROP TABLE IF EXISTS db.public.teams;
		DROP TABLE IF EXISTS db.public.training;
		DROP TABLE IF EXISTS db.public.cpu_utilization;
		DROP TABLE IF EXISTS db.public.observations;
		DROP TABLE IF EXISTS db.public.time_monitor;
		DROP TABLE IF EXISTS db.public.diversity_cache;
		DROP TABLE IF EXISTS db.public.compute_configs;
		""")

    @staticmethod
    def add_time_monitor_data(run_id, generation, time):
        duckdb.sql(
            f"INSERT INTO db.public.time_monitor (run_id, generation, time) VALUES ('{run_id}', {generation}, {time});")

    @staticmethod
    def add_cpu_utilization_data(data):
        for row in data:
            run_id = row['run_id']
            time = row['time']
            worker = row['worker']
            core = row['core']
            utilization = row['utilization']
            query = f"INSERT INTO db.public.cpu_utilization (run_id, time, worker, core, utilization) VALUES ('{run_id}', {time}, '{worker}', {core}, {utilization});"
            duckdb.sql(query)

    @staticmethod
    def add_training_data(data):
        placeholders = ','.join(['?' for _ in range(len(data[0]))])
        sql_query = f"INSERT INTO db.public.training VALUES ({placeholders})"

        values = [
            (row['run_id'], row['generation'], row['team_id'], row['is_finished'], row['reward'], row['time_step'],
             row['time'], row['action'])
            for row in data
        ]

        duckdb.executemany(sql_query, values)

    @staticmethod
    def add_compute_config(run_id, team_distribution, batch_sizes):
        query = f"INSERT INTO db.public.compute_configs VALUES ('{run_id}', '{team_distribution}', '{batch_sizes}');"
        print(query)
        duckdb.sql(query)

    @staticmethod
    def add_team(run_id, team):
        duckdb.sql(f"INSERT INTO db.public.teams (run_id, id, lucky_breaks) VALUES ('{run_id}', '{team.id}', 0);")

    @staticmethod
    def remove_team(run_id, team):
        duckdb.sql(f"DELETE FROM db.public.programs WHERE team_id = '{team.id}' AND run_id = '{run_id}';")
        duckdb.sql(f"DELETE FROM db.public.teams WHERE id = '{team.id}' AND run_id = '{run_id}';")

    @staticmethod
    def remove_program(run_id, program, team):
        duckdb.sql(
            f"DELETE FROM db.public.programs WHERE run_id = '{run_id}' AND id = '{program.id}' AND team_id = '{team.id}';")

    @staticmethod
    def add_program(run_id, program, team):
        duckdb.sql(f"""
            INSERT INTO db.public.programs (run_id, id, team_id, action, pointer)
            VALUES ('{run_id}', '{program.id}', '{team.id}', '{program.action}', NULL);""")

    @staticmethod
    def update_program(run_id, program, team, action, pointer):
        if not action:
            action = "NULL"
        else:
            action = f"'{action}'"

        if not pointer:
            pointer = "NULL"
        else:
            pointer = f"'{pointer}'"

        duckdb.sql(f"""
        UPDATE db.public.programs
        SET
            action = {action},
            pointer = {pointer}
        WHERE run_id = '{run_id}'
        AND id = '{program.id}'
        AND team_id = '{team.id}'""")

    @staticmethod
    def get_teams(run_id):
        return duckdb.sql(f"""SELECT * FROM db.public.teams WHERE run_id = '{run_id}'""")

    @staticmethod
    def get_root_teams(run_id):
        return duckdb.sql(f"""
            WITH programs_pointing_to_teams AS (
                SELECT pointer FROM db.public.programs 
                WHERE pointer IS NOT NULL
                AND run_id = '{run_id}'
            )

            SELECT * FROM db.public.teams
            WHERE id NOT IN (SELECT pointer FROM programs_pointing_to_teams)
            AND run_id = '{run_id}'
            """).df()['id'].tolist()

    @staticmethod
    def get_ranked_teams(run_id, generation):
        return duckdb.sql(f"""
                WITH team_cumulative_rewards AS (
                  SELECT generation,
                         team_id,
                         SUM(reward) AS cumulative_reward
                  FROM db.public.training
                  WHERE generation = '{generation}'
                  AND run_id = '{run_id}'
                  GROUP BY generation, team_id
				)

				SELECT generation,
			           team_id,
					   cumulative_reward,
					   ROW_NUMBER() OVER (PARTITION BY generation ORDER BY cumulative_reward DESC) AS rank
				FROM team_cumulative_rewards
				WHERE generation={generation}""").df()

    @staticmethod
    def update_team(run_id, team, lucky_breaks):
        return duckdb.sql(f"""
        UPDATE db.public.teams SET lucky_breaks = '{lucky_breaks}' 
        WHERE id = '{team.id}'
        AND run_id = '{run_id}';
        """)

    @staticmethod
    def add_observation(run_id, time, observation):
        observation = ', '.join(map(str, observation))

        query = f"""
        INSERT INTO db.public.observations (run_id, time, observation)
        VALUES ('{run_id}', {time}, ARRAY[{observation}])
       """

        return duckdb.sql(query)

    @staticmethod
    def add_profile(run_id, team, time, profile):
        profile = ', '.join(map(str, profile))

        query = f"""
        INSERT INTO db.public.diversity_cache (run_id, team_id, time, profile)
        VALUES ('{run_id}', '{team.id}', {time}, ARRAY[{profile}])
        """

        return duckdb.sql(query)

    @staticmethod
    def get_diversity_cache(run_id):
        return duckdb.query(f"""
        SELECT * FROM db.public.observations
        WHERE run_id = '{run_id}'
        ORDER BY time DESC
        LIMIT {Parameters.DIVERSITY_CACHE_SIZE} 
        """).df()['observation'].to_list()

    @staticmethod
    def get_diversity_profiles(run_id):
        profiles = []

        query_results = duckdb.query(f"""
            SELECT profile FROM db.public.diversity_cache
            WHERE run_id = '{run_id}'
            ORDER BY time DESC
            LIMIT {Parameters.DIVERSITY_CACHE_SIZE}
        """).df()['profile']

        for profile in query_results:
            if isinstance(profile, np.ndarray):
                profile = profile.tolist()
            profiles.append(profile)

        return profiles
