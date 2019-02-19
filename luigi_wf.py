import luigi

from gdtc_base.gdtc_core.insert_hdf import insertHDF
from gdtc_base.gdtc_base.db import Db
from gdtc_luigi.targets.postgres_target import PostgresTarget
from gdtc_luigi.gdtc_base.config_env import ConfigEnv

db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters', '21')

class workflow(luigi.Task):

    file_name = 'MCD12Q1.A2007001.h17v04.006.2018054172555'

    def requires(self):
        return ConfigEnv(self.file_name + '.hdf')

    def output(self):
        return PostgresTarget(db)

    def run(self):
        insertHDF(self.file_name, db=db, coord_sys="4326").run()

# Build and run workflow
if __name__ == '__main__':

    luigi.build([workflow()],local_scheduler=False, log_level="DEBUG")
    luigi.run()