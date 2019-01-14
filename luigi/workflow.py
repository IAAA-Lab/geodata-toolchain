import luigi
from load import insertHDF
from DB import Db

db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters2', '5')

# Run workflow
if __name__ == '__main__':
    luigi.build([insertHDF(file_name='MCD12Q1.A2006001.h17v04.006.2018054121935', db=db)], local_scheduler=True)
    luigi.run()