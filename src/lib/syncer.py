import json
from src.lib.db import DB
from psycopg2.extras import RealDictCursor
from datetime import datetime


class Syncer:
    def __init__(self, source_db: DB, target_db: DB):
        self.source_db = source_db
        self.target_db = target_db

    def check_last_sync_date(
        self,
        target_cur: RealDictCursor
    ) -> datetime:
        sql = f"SELECT synced_at FROM {self.target_db.schema}.last_sync_date ORDER BY 1 DESC"
        target_cur.execute(sql)
        result = target_cur.fetchone()
        if result:
            return result["synced_at"]
        return datetime(1970, 1, 1, 0, 0, 0)

    def update_last_sync_date(
        self,
        target_cur: RealDictCursor,
        rows_inserted: int
    ) -> None:
        sql = f"INSERT INTO {self.target_db.schema}.last_sync_date (synced_at, rows_inserted) VALUES (NOW(), %(rows_inserted)s)"
        target_cur.execute(sql, {"rows_inserted": rows_inserted})

    def check_table_fields(
        self,
        table_name: str,
        source_cur: RealDictCursor,
        target_cur: RealDictCursor
    ) -> list:
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
        return [x["column_name"] for x in json.loads(source_result)]

    def sync_table(
        self,
        table_name: str,
        source_cur: RealDictCursor,
        target_cur: RealDictCursor,
        last_sync_date: datetime
    ) -> int:
        """
            syncs the data for a given table_name from the source DB to the target DB

        :return: number of rows inserted
        """
        # TODO:
        #   - check the last sync date
        #   - select the data > last sync date
        #   - insert it into the target db
        fields = self.check_table_fields(table_name, source_cur, target_cur)
        print(fields)
        return 123

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
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur,\
            self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            last_sync_date = self.check_last_sync_date(target_cur)
            print(f"last sync date = {last_sync_date}")
            rows_inserted = 0
            for table in tables:
                rows_inserted += self.sync_table(table, source_cur, target_cur)
            self.update_last_sync_date(target_cur, rows_inserted)