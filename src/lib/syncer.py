import json
from psycopg2.extras import RealDictCursor, execute_values
from datetime import datetime
from src.lib.db import DB
from src.lib.exceptions.InconsistentTablesException import InconsistentTablesException
from src.lib.exceptions.InconsistentNumberOfRowsException import InconsistentNumberOfRowsException


class Syncer:
    def __init__(self, source_db: DB, target_db: DB, batch_size: int = 10, last_sync_date_table="last_sync_date"):
        self.source_db = source_db
        self.target_db = target_db
        self.batch_size = batch_size
        self.last_sync_date_table = last_sync_date_table

    def check_last_sync_date(
        self,
        target_cur: RealDictCursor
    ) -> datetime:
        """
            Returns the last synchronization date.
            If the table is empty, returns the beginning of the Unix epoch (1970-01-01 00:00).
        :param target_cur:
        :return:
        """
        sql = f"SELECT synced_at FROM {self.target_db.schema}.{self.last_sync_date_table} ORDER BY 1 DESC"
        target_cur.execute(sql)
        result = target_cur.fetchone()
        if result:
            return result["synced_at"]
        return datetime(1970, 1, 1, 0, 0, 0)

    def update_last_sync_date(
        self,
        target_cur: RealDictCursor,
        rows_inserted: int,
        sync_date: datetime = datetime.now(),
    ) -> None:
        """
            Inserts a row denoting the last synchronization date, along with the number of rows inserted in the sync
        :param sync_date:
        :param target_cur:
        :param rows_inserted:
        :return:
        """
        sql = f"INSERT INTO {self.target_db.schema}.{self.last_sync_date_table} (synced_at, rows_inserted) " \
              f"VALUES (%(sync_date)s, %(rows_inserted)s)"
        target_cur.execute(sql, {"sync_date": sync_date, "rows_inserted": rows_inserted})

    def check_table_fields(
        self,
        table_name: str,
        source_cur: RealDictCursor,
        target_cur: RealDictCursor
    ) -> list:
        """
            Checks whether the source and the target tables have consistent fields.
            Fails if there is an inconsistency, otherwise returns the names of the columns.
        :param table_name:
        :param source_cur:
        :param target_cur:
        :return: column names which need to be synced
        """
        sql = f"SELECT column_name, data_type, character_maximum_length " \
              f"FROM information_schema.columns " \
              f"WHERE table_name IN (%(table_name)s) AND table_schema IN (%(table_schema)s);"

        source_cur.execute(sql, {"table_name": table_name, "table_schema": self.source_db.schema})
        source_result = json.dumps(source_cur.fetchall(), sort_keys=True)

        target_cur.execute(sql, {"table_name": table_name, "table_schema": self.target_db.schema})
        target_result = json.dumps(target_cur.fetchall(), sort_keys=True)

        if source_result != target_result:
            raise InconsistentTablesException(f"Source and target tables have colum discrepancies:" \
                                               f"\n source: {source_result}," \
                                               f"\n target: {target_result}")
        return [x["column_name"] for x in json.loads(source_result)]

    def check_table_counts(
        self,
        table_name: str,
        source_cur: RealDictCursor,
        target_cur: RealDictCursor
    ) -> int:
        """
            Checks the row count of the source and the target table.
            It throws an exception if the count is inconsistent.
            Most probable cause is that the last sync date was not reset after the target table has been truncated.
            NOTE: Does not check contents!!!
        :param table_name:
        :param source_cur:
        :param target_cur:
        :return: the number of rows in both of the tables
        """
        count_source_sql = f"SELECT COUNT(*) FROM {self.source_db.schema}.{table_name}"
        source_cur.execute(count_source_sql)
        count_source = source_cur.fetchone()["count"]

        count_target_sql = f"SELECT COUNT(*) FROM {self.target_db.schema}.{table_name}"
        target_cur.execute(count_target_sql)
        count_target = target_cur.fetchone()["count"]

        if count_source != count_target:
            raise InconsistentNumberOfRowsException(f"Source and target tables have inconsistent number of rows " \
                                             f"source = {count_source} vs target = {count_target}.")
        return count_target

    def sync_table(
        self,
        table_name: str,
        source_cur: RealDictCursor,
        target_cur: RealDictCursor,
        last_sync_date: datetime
    ) -> int:
        """
            Syncs the data for a given table_name from the source DB to the target DB
            - checks if the fields in source and target are matching
            - selects the data > last sync date from source db
            - inserts it into the target db
            - asserts counts in both source and target are the same

        :return: number of rows inserted
        """

        fields = self.check_table_fields(table_name, source_cur, target_cur)
        select_sql = f"SELECT {', '.join(fields)} " \
                     f"FROM {self.source_db.schema}.{table_name} " \
                     f"WHERE created_at > '{last_sync_date}'"  # potential pitfall, using timestamp vs date
        insert_sql = f"INSERT INTO {self.target_db.schema}.{table_name} " \
                     f" ({', '.join(fields)}) " \
                     f" VALUES %s;"
        source_cur.execute(select_sql)

        rows_inserted = 0
        while True:
            batch = source_cur.fetchmany(self.batch_size)
            if not batch:
                break
            rows_inserted += len(batch)
            vals = [tuple(row[f] for f in fields) for row in batch]
            execute_values(target_cur, insert_sql, vals)

        self.check_table_counts(table_name, source_cur, target_cur)

        return rows_inserted

    def sync(self, tables: list) -> int:
        """
            syncs a list of tables from the source to the target DB
                - opens a transaction in both source and target dbs
                - checks the last sync date
                - syncs the tables
                - asserts everything is ok
                - updates the last sync date
        :param tables: the tables which need to be synced from the source to the target DB
        :return:
        """
        print(f"Initiating the sync process...")
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            last_sync_date = self.check_last_sync_date(target_cur)
            print(f"The last sync date is {last_sync_date}")
            rows_inserted = 0
            for table in tables:
                rows_inserted += self.sync_table(table, source_cur, target_cur, last_sync_date)
            self.update_last_sync_date(target_cur, rows_inserted)
            print(f"Table sync is done, synced {rows_inserted} rows in total.")
            return rows_inserted
