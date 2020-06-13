import json
from src.lib.db import DB
from psycopg2.extras import RealDictCursor


class Syncer:
    def __init__(self, source_db: DB, target_db: DB):
        self.source_db = source_db
        self.target_db = target_db

    def check_fields(
        self,
        table_name: str,
        source_cur: RealDictCursor,
        target_cur: RealDictCursor
    ):
        sql = f"SELECT column_name, data_type, character_maximum_length " \
              f"FROM information_schema.columns " \
              f"WHERE table_name IN (%(table_name)s) AND table_schema IN (%(table_schema)s);"

        source_cur.execute(sql, {"table_name": table_name, "table_schema": self.source_db.schema})
        source_result = json.dumps(source_cur.fetchall(), sort_keys=True)

        target_cur.execute(sql, {"table_name": table_name, "table_schema": self.target_db.schema})
        target_result = json.dumps(target_cur.fetchall(), sort_keys=True)

        assert source_result == target_result, f"Source and target tables have colum discrepancies:" \
                                               f"\n source: {source_result}," \
                                               f"\n target: {target_result}"

    def sync_table(
        self,
        table_name: str,
        source_cur: RealDictCursor,
        target_cur: RealDictCursor
    ):
        """
            syncs the data for a given table_name from the source DB to the target DB

        :return:
        """
        # TODO:
        #   - check the last sync date
        #   - select the data > last sync date
        #   - insert it into the target db
        self.check_fields(table_name, source_cur, target_cur)

    def sync(self, tables: list):
        """
            syncs a list of tables from the source to the target DB

        :param tables: the tables which need to be synced from the source to the target DB
        :return:
        """
        # TODO:
        #   - open a transaction in both source and target dbs
        #   - sync the tables
        #       - check if the fields in source and target are matching
        #   - assert everything is ok
        #       - counts in both source and target are the same
        #   - update the last sync date
        with self.target_db.connect() as target_conn, target_conn.cursor() as target_cur, \
                self.source_db.connect() as source_conn, source_conn.cursor() as source_cur:
            for table in tables:
                self.sync_table(table, source_cur, target_cur)
