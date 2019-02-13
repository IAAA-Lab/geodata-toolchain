import luigi

from gdtc.gdtc_core.insert_hdf import insertHDF
from gdtc.gdtc_base.db import Db

# Workflow example

# Final db
db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters', '8')

# Build and run workflow
if __name__ == '__main__':
    luigi.build([
            insertHDF(file_name='MCD12Q1.A2006001.h17v04.006.2018054121935', db=db),
            insertHDF(file_name='MCD12Q1.A2007001.h17v04.006.2018054172555', db=db)
    ],local_scheduler=False, log_level="DEBUG")

    luigi.run()
