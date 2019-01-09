import luigi
from PostgresTarget import PostgresTarget
from DB import Db

# Define Task for inserting HDF file
class insertHDF(luigi.Task):
    """
    Inserts an HDF file into the target Db
    """

    task_namespace = 'input'

    def requires(self):
        pass

    def output(self):
        db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'test', '14')
        return PostgresTarget(db)
    
    def run(self):
        sql = """INSERT INTO test (col) VALUES (101); SELECT * FROM test;"""
        print(self.output().connect().executeQuery(sql).result)

# Run workflow
if __name__ == '__main__':
    luigi.run(['input.insertHDF', '--workers', '1', '--local-scheduler'])
