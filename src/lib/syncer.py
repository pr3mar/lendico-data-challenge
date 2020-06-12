from src.lib.db import DB


class Syncer:
    def __init__(self, source: DB, target: DB):
        self.source = source
        self.target = target

    def sync_table(self, table_name: str):
        """
            syncs the data for a given table_name from the source DB to the target DB

        :return:
        """
        # TODO:
        #   - check the last sync date
        #   - select the data > last sync date
        #   - insert it into the target db
        pass

    def sync(self, tables: list):
        """
            syncs a list of tables from the source to the target DB

        :param tables: the tables which need to be synced from the source to the target DB
        :return:
        """
        # TODO:
        #   - open a transaction in both source and target dbs
        #   - sync the tables
        #   - assert everything is ok
        #   - update the last sync date

        pass
