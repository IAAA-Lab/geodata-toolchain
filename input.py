import luigi
import luigi.contrib.postgres
from DB import DB

class InsertHDF(luigi.Task):
    task_namespace = 'input'

    def __init__(self, db: DB):
        self.db = db

    def requires(self):
        return luigi.contrib.postgres.PostgresTarget(
            host = self.db.host,
            port = self.db.port,
            user = self.db.user,
            password = self.db.password,
            database = self.db.db,
            table = self.db.table,
            update_id = self.db.update_id
        )

    def run(self):
        self.requires().connect()

if __name__ == '__main__':
    luigi.run(['input.InsertHDF', '--workers', '2', '--local-scheduler'])
