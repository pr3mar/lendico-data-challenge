from src.lib.syncer import Syncer
from src.lib.db import DB


if __name__ == '__main__':
    print("HelloWorld")
    source_db = DB("source", "postgres", "admin").connect()
    target_db = DB("target", "postgres", "admin").connect()

    syncer = Syncer(source=source_db, target=target_db)
    syncer.sync(["address", "company"])
