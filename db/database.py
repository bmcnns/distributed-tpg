import psycopg2 as pg
from psycopg2 import Error

class Database:
    schemas = [
        """
        CREATE TABLE IF NOT EXISTS training (
            generation INT,
            environment_id INT,
            team_id INT,
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
                    id SERIAL PRIMARY KEY
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS programs (
                id SERIAL PRIMARY KEY, 
                team_id INT,
                action INT,
                UNIQUE (id, team_id),
                FOREIGN KEY (team_id) REFERENCES teams(id)
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS instructions (
                id SERIAL PRIMARY KEY,
                program_id INT,
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
    def load(cls):

        if not cls.connection:
            print("Not connected to the database. Please connect to PostgreSQL first with Database.connect()")
            return

        cursor = cls.connection.cursor()

        # Initialize the tables
        for schema in cls.schemas:
            cursor.execute(schema)

        # Commit the changes and close connection
        cls.connection.commit()
        cursor.close()
        print("Database and tables created successfully")