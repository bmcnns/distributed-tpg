import psycopg2 as pg
from psycopg2 import Error
from psycopg2 import sql
import pandas as pd

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
                action INT,
                UNIQUE (id, team_id),
                FOREIGN KEY (team_id) REFERENCES teams(id)
            )
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
            
