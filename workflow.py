from GDTC.Extraction import Extraction
from GDTC.DB import DB

db_src = DB('127.0.0.1', '5432', 'postgres', 'password', 'postgres', 'geo-rasters')
wf = Extraction()

wf.insertHDF('MCD12Q1.A2006001.h17v04.006.2018054121935.hdf', 0, db_src)
