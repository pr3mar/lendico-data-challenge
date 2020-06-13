from src.lib.syncer import Syncer
from src.lib.db import DB


if __name__ == '__main__':
    source_db = DB(db_name="source", username="postgres", password="admin")
    target_db = DB(db_name="target", username="postgres", password="admin")

    syncer = Syncer(source_db=source_db, target_db=target_db)
    syncer.sync(["address", "company"])
