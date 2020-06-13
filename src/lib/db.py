from psycopg2 import connect
from psycopg2.extras import RealDictCursor, RealDictConnection


class DB:
    def __init__(
            self,
            db_name: str,
            username: str,
            password: str,
            host: str = "localhost",
            port: int = 5432,
            schema: str = "public",
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.schema = schema
        self.username = username
        self.password = password

    def connect(self) -> RealDictConnection:
        try:
            return connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.username,
                password=self.password,
                connection_factory=RealDictConnection,
                cursor_factory=RealDictCursor
            )
        except Exception as e:
            print(f"Error connecting to the database {e}")
