from src.lib.syncer import Syncer
from src.lib.db import DB


if __name__ == '__main__':
    print("HelloWorld")
    source_db = DB("source", "postgres", "admin")
    target_db = DB("target", "postgres", "admin")

    syncer = Syncer(source_db=source_db, target_db=target_db)
    syncer.sync(["address", "company"])
