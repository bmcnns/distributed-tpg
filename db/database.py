import psycopg2 as pg
from psycopg2 import Error
from psycopg2 import sql
import pandas as pd
import duckdb

class Database:

	schemas = [
		"""
		CREATE TABLE IF NOT EXISTS training (
			generation INT,
			environment_id INT,
			team_id UUID,
			is_terminated BOOLEAN,
			is_truncated BOOLEAN,
			reward FLOAT8,
			time_step INT,
			action INT,
			PRIMARY KEY (generation, team_id, time_step)
		)
		""",
		"""
			CREATE TABLE IF NOT EXISTS teams (
					id UUID PRIMARY KEY
			)
		""",
		"""
			CREATE TABLE IF NOT EXISTS programs (
				id UUID PRIMARY KEY, 
				team_id UUID,
				action VARCHAR,
				pointer UUID,
				UNIQUE (id, team_id),
				CONSTRAINT action_or_pointer_check CHECK (
						(action IS NOT NULL AND pointer IS NULL) OR
						(action IS NULL AND pointer IS NOT NULL)
				),
				FOREIGN KEY (team_id) REFERENCES teams(id)
			);
		""",
		"""
			CREATE TABLE IF NOT EXISTS instructions (
				id UUID PRIMARY KEY,
				program_id UUID,
				mode VARCHAR,
				operation VARCHAR,
				source INT,
				destination INT,
				FOREIGN KEY (program_id) REFERENCES programs(id)
			) 
		"""
	]

	connection = None
	
	@classmethod
	def connect(cls, user, password, host, port, database):
		if cls.connection is not None:
			return cls.connection
		else:
			try:
				cls.connection = pg.connect(
					user=user,
					password=password,
					host=host,
					port=port,
					database=database
				)

				return cls.connection

			except (Exception, Error) as error:
				print("Error while connecting to database", error)

	@classmethod
	def clear(cls):
		if not cls.connection:
			print("Not connected to the database. Please connect to Postgresql first with database.connect()")
			return

		cursor = cls.connection.cursor()

		query = """
		DROP TABLE IF EXISTS instructions;
		DROP TABLE IF EXISTS programs;
		DROP TABLE IF EXISTS teams;
		DROP TABLE IF EXISTS training;
		"""

		cursor.execute(query)

		# Commit the changes and close connection
		cls.connection.commit()
		cursor.close()

	@classmethod
	def load(cls):

		if not cls.connection:
			print("not connected to the database. please connect to postgresql first with database.connect()")
			return

		cursor = cls.connection.cursor()

		# initialize the tables
		for schema in cls.schemas:
			cursor.execute(schema)

		# Commit the changes and close connection
		cls.connection.commit()
		cursor.close()
		#print("Database and tables created successfully")

	@classmethod
	def store(cls, table_name, df):
		if not cls.connection:
			print("Not connected to the database. Please connect to PostgreSQL first with Database.connect()")
			return

		try:
			cursor = cls.connection.cursor()
			
			columns = df.columns.tolist()
			insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
				sql.Identifier(table_name),
				sql.SQL(', ').join(map(sql.Identifier, columns)),
				sql.SQL(', ').join(sql.Placeholder() * len(columns))
			)

			for row in df.itertuples(index=False, name=None):
				cursor.execute(insert_query, row)

			# Commit the transaction
			cls.connection.commit()
			print(f"Data inserted successfully into {table_name}.")

		except Exception as error:
			print(f"Error while inserting data: {error}")
		finally:
			if cursor:
				cursor.close()
				
	@staticmethod
	def connect_duckdb():
		duckdb.sql("INSTALL postgres;")
		duckdb.sql("LOAD postgres;")
		duckdb.sql("ATTACH 'dbname=postgres user=postgres host=192.168.4.122 password=template!PWD' AS db (TYPE POSTGRES);")

	@staticmethod
	def add_team(team):
		duckdb.sql(f"INSERT INTO db.public.teams (id) VALUES ('{team.id}');")

	@staticmethod
	def remove_team(team):
		duckdb.sql(f"DELETE FROM db.public.programs WHERE team_id = '{team.id}';")
		duckdb.sql(f"DELETE FROM db.public.teams WHERE id = '{team.id}';")

	@staticmethod
	def add_program(program):
		duckdb.sql(f"INSERT INTO db.public.programs (id, team_id, action, pointer) VALUES ('{program.id}', NULL, '{program.action}', NULL);")

	@staticmethod
	def update_program_team_id(program, team):
		duckdb.sql(f"UPDATE db.public.programs SET team_id = '{team.id}' WHERE id = '{program.id}';")

	@staticmethod
	def update_program_action(program):
		duckdb.sql(f"UPDATE db.public.programs SET action = '{program.action}' WHERE ID = '{program.id}';")

	@staticmethod
	def get_teams():
		return duckdb.sql("""SELECT * FROM db.public.teams""")

	@staticmethod
	def get_root_teams():
		return duckdb.sql(f"""
			WITH programs_pointing_to_teams AS (
			SELECT pointer FROM db.public.programs WHERE pointer IS NOT NULL 
			)
			SELECT * FROM db.public.teams 
			WHERE id NOT IN (SELECT pointer FROM programs_pointing_to_teams)
		""").df()['id'].tolist()

	@staticmethod
	def clean_unused_programs():
		duckdb.sql("""
			DELETE FROM db.public.instructions
			WHERE program_id IN (SELECT program_id FROM db.public.programs WHERE team_id IS NULL);
		""")

		duckdb.sql("""
			DELETE FROM db.public.programs WHERE team_id IS NULL;
		""")

	@staticmethod
	def get_ranked_teams():
		return duckdb.sql("""
				WITH team_cumulative_rewards AS (
						SELECT generation,
								     team_id,
								     SUM(reward) AS cumulative_reward
						FROM db.public.training
						GROUP BY generation, team_id
				)

				SELECT generation,
						team_id,
						cumulative_reward,
						ROW_NUMBER() OVER (PARTITION BY generation ORDER BY cumulative_reward DESC) AS rank
				FROM team_cumulative_rewards""")

