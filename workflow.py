# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Workflow

from GDTC.DB import DB
from GDTC.Pipe import Pipe
from GDTC.Extraction import Extraction as e
from GDTC.Transformation import Transformation as t
from GDTC.Load import Load as l

# Define variables
db = DB('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters')
pipe = Pipe(db)
layers = [0, 1, 2, 5, 7]

# Insert layers from HDF file
for layer in layers:
    pipe = e.insertHDF('MCD12Q1.A2006001.h17v04.006.2018054121935.hdf', layer,'', pipe)

# Workflow
pipe = e.insertSHP('Comunidades_Autonomas_ETRS89_30N', '', pipe)
pipe = e.getHDF('MCD12Q1.A2006001.h17v04.006.2018054121935.hdf.tif', pipe)

pipe = t.clipHDFwithSHP('file_name1', 'file_name2', pipe)

pipe = l.loadHDF(pipe)
