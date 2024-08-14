from psycopg2 import sql

class Queries:
  @staticmethod
  def select_all(table_name):
      query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
      return query
