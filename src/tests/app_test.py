import unittest
from datetime import datetime
from src.lib.db import DB
from src.lib.syncer import Syncer
from src.lib.exceptions.InconsistentTablesException import InconsistentTablesException
from src.lib.exceptions.InconsistentNumberOfRowsException import InconsistentNumberOfRowsException


class TestApp(unittest.TestCase):

    def setUp(self) -> None:
        self.target_db = DB(db_name="target", username="postgres", password="admin")
        self.source_db = DB(db_name="source", username="postgres", password="admin")
        self.last_sync_date_table = "last_sync_date_test"
        self.sync_table = "sync_table_test"
        self.syncer = Syncer(source_db=self.source_db, target_db=self.target_db,
                             last_sync_date_table="last_sync_date_test")
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            source_cur.execute(f"CREATE TABLE IF NOT EXISTS {self.sync_table}(id int, created_at date);")
            target_cur.execute(f"CREATE TABLE IF NOT EXISTS {self.sync_table}(id int, created_at date);")
            target_cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.last_sync_date_table}(synced_at TIMESTAMP, rows_inserted int);")

    def tearDown(self) -> None:
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            source_cur.execute(f"DROP TABLE IF EXISTS {self.sync_table};")
            target_cur.execute(f"DROP TABLE IF EXISTS {self.sync_table};")
            target_cur.execute(f"DROP TABLE IF EXISTS {self.last_sync_date_table};")

    def test_check_last_sync_date_no_data(self):
        # test an empty table
        with self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            sync_date = self.syncer.check_last_sync_date(target_cur)
            self.assertEqual(sync_date, datetime(1970, 1, 1, 0, 0))

    def test_check_last_sync_date_existing_data(self):
        # test with existing data
        with self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            target_cur.execute(f"INSERT INTO {self.last_sync_date_table} (synced_at, rows_inserted) VALUES "
                               f"('2020-06-06 00:00', 1000), "
                               f"('2020-06-09 00:00', 30), "
                               f"('2020-06-10 00:00', 100);")
            sync_date = self.syncer.check_last_sync_date(target_cur)
            self.assertEqual(sync_date, datetime(2020, 6, 10, 0, 0))

    def test_update_last_sync_date(self):
        with self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            sync_date = datetime.now()
            self.syncer.update_last_sync_date(target_cur, rows_inserted=100, sync_date=sync_date)
            target_cur.execute(f"SELECT * FROM {self.last_sync_date_table} ORDER BY synced_at DESC")
            db_sync_date = target_cur.fetchone()["synced_at"]
            self.assertEqual(sync_date, db_sync_date)

    def test_check_table_fields_consistent_tables(self):
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            try:
                fields = self.syncer.check_table_fields(self.sync_table, source_cur, target_cur)
                self.assertEqual(fields, ["id", "created_at"])
            except InconsistentTablesException:
                self.fail("Should not have failed here, tables are consistent.")

    def test_check_table_fields_inconsistent_tables(self):
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur, \
                self.assertRaises(InconsistentTablesException) as ctx:
            source_cur.execute(f"ALTER TABLE {self.sync_table} ADD COLUMN dummy_col int")
            self.syncer.check_table_fields(self.sync_table, source_cur, target_cur)
            self.assertTrue("dummy_col" in ctx.exception)

    def test_check_table_counts_consistent(self):
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            try:
                source_cur.execute(
                    f"INSERT INTO {self.sync_table} (id, created_at) VALUES (1, '2020-06-10'), (2, '2020-06-10'), (3, '2020-06-10')")
                target_cur.execute(
                    f"INSERT INTO {self.sync_table} (id, created_at) VALUES (1, '2020-06-10'), (2, '2020-06-10'), (3, '2020-06-10')")
                num_rows = self.syncer.check_table_counts(self.sync_table, source_cur, target_cur)
                self.assertEqual(num_rows, 3)
            except InconsistentNumberOfRowsException:
                self.fail("Tables should have consistent number of rows.")

    def test_check_table_counts_inconsistent(self):
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur, \
                self.assertRaises(InconsistentNumberOfRowsException) as ctx:
            target_cur.execute(f"INSERT INTO {self.sync_table} (id, created_at) VALUES (1, '2020-06-10')")
            self.syncer.check_table_counts(self.sync_table, source_cur, target_cur)
            self.assertTrue("source: 0" in ctx.exception and "target: 1" in ctx.exception)

    def test_sync_table_complete(self):
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            try:
                last_sync_date = datetime(2020, 6, 8, 0, 0)
                source_cur.execute(f"INSERT INTO {self.sync_table} (id, created_at) VALUES "
                                   f"(1, '2020-06-09'), "
                                   f"(2, '2020-06-10'), "
                                   f"(3, '2020-06-10'),"
                                   f"(4, '2020-06-10')")
                synced_rows = self.syncer.sync_table(self.sync_table, source_cur, target_cur, last_sync_date)
                self.assertEqual(4, synced_rows)
            except Exception:
                self.fail("It should sync the tables.")

    def test_sync_table_incomplete_sync(self):
        # the number of rows after each sync should be the same. If there are inconsistencies => fail
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur, \
                self.assertRaises(InconsistentNumberOfRowsException) as ctx:
            last_sync_date = datetime(2020, 6, 9, 0, 0)
            source_cur.execute(f"INSERT INTO {self.sync_table} (id, created_at) VALUES "
                               f"(1, '2020-06-09'), "  # created_at is not greater than last_sync_date -> fails 
                               f"(2, '2020-06-10'), "
                               f"(3, '2020-06-10'),"
                               f"(4, '2020-06-10')")
            self.syncer.sync_table(self.sync_table, source_cur, target_cur, last_sync_date)

    def __prepare_data(self):
        with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:
            source_cur.execute(f"INSERT INTO public.{self.sync_table} (id, created_at) VALUES "
                               f"(1, '2020-06-09'), "
                               f"(2, '2020-06-10'), "
                               f"(3, '2020-06-10'), "
                               f"(4, '2020-06-10')")

    def test_sync(self):
        self.__prepare_data()
        try:
            num_synced = self.syncer.sync([self.sync_table])
            self.assertEqual(num_synced, 4)
            with self.source_db.connect() as source_conn, source_conn.cursor() as source_cur, \
                    self.target_db.connect() as target_conn, target_conn.cursor() as target_cur:

                target_cur.execute(f"SELECT * FROM public.{self.sync_table}")
                synced_data = target_cur.fetchall()

                source_cur.execute(f"SELECT * FROM public.{self.sync_table}")
                source_data = source_cur.fetchall()

                target_cur.execute(f"SELECT * FROM public.{self.last_sync_date_table}")
                last_sync_target = target_cur.fetchall()

                self.assertDictEqual(dict(source_data), dict(synced_data))
                self.assertEqual(len(last_sync_target), 1)
                self.assertTrue((datetime.now() - last_sync_target[0]["synced_at"]).seconds < 1)
                self.assertEqual(last_sync_target[0]["rows_inserted"], 4)
        except Exception as e:
            self.fail("It should sync the tables.")


if __name__ == '__main__':
    unittest.main()
