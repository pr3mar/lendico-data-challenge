from psycopg2 import connect
from psycopg2.extras import RealDictCursor


class DB:
    def __init__(self, db_name: str, username: str, password: str, host: str = "localhost", port: int = 5432):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password

    def connect(self):
        try:
            return connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.username,
                password=self.password,
                cursor_factory=RealDictCursor
            )
        except Exception as e:
            print(f"Error connecting to the database {e}")
