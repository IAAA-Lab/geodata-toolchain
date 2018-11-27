# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Workflow

from GDTC.Extraction import Extraction
from GDTC.DB import DB

# Define DB
db_src = DB('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters')
wf = Extraction()

# Define Workflow
wf.insertHDF('MCD12Q1.A2006001.h17v04.006.2018054121935.hdf', 0, db_src, '')
wf.insertSHP('Comunidades_Autonomas_ETRS89_30N', db_src, '')
hdfFile = wf.getHDF('MCD12Q1.A2006001.h17v04.006.2018054121935.hdf.tif', db_src)
