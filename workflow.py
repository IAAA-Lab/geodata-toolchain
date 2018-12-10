# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Workflow
# Description: This module is where the user can define a complete workflow composed by one or many filters.

from GDTC.Pipe import Pipe
import GDTC.Filters.input as i
import GDTC.Filters.output as o
from GDTC.Stage import Stage
from GDTC.decorators import run

# Define initial Pipe
pipe = Pipe('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters')

# First approach of functional style workflow
# It's not possible to instpect this because lambda functions has no name or docstring

s1 = Stage(pipe)
s1.add(lambda: o.loadHDF(pipe, 'MCD12Q1.A2006001.h17v04.006.2018054121935.hdf', 1,''))\
  .add(lambda: o.loadSHP(pipe, 'Comunidades_Autonomas_ETRS89_30N'))\
  .add(lambda: o.loadSHP(pipe, 'Comunidades_Autonomas_ETRS89_30N'))\
# .run()

# Second approach of functional style workflow
# This approach is better because it lets the inspection of the function and it's
# easier to hide the use of pipes. It's also easier for the user beacuse there's no
# need to use lambda functions

s2 = Stage(pipe)
s2.configure('concurrent')\
  .add(o.loadHDF, 'MCD12Q1.A2006001.h17v04.006.2018054121935.hdf', 1)\
  .add(o.loadHDF, 'Comunidades_Autonomas_ETRS89_30N')\
  .add(o.loadHDF, 'Comunidades_Autonomas_ETRS89_30N')\
  .inspect()\
  .run()
