from GDTC.Extraction import Extraction
from GDTC.DB import DB

db_src = DB('35.230.154.144', '5432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters')
#db_src = DB('35.230.154.144', '5432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters')
wf = Extraction()

#wf.insertHDF('MCD12Q1.A2006001.h17v04.006.2018054121935.hdf', 0, db_src, '-a')
# hdfFile = wf.getHDF('MCD12Q1.A2006001.h17v04.006.2018054121935.hdf.tif', db_src)
wf.insertSHP('Comunidades_Autonomas_ETRS89_30N', db_src, '')
