import luigi
from load import insertHDF

# Run workflow
if __name__ == '__main__':
    luigi.build([insertHDF('MCD12Q1.A2006001.h17v04.006.2018054121935')], local_scheduler=True)
    luigi.run()