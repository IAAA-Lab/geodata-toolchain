# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Load

from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call
from GDTC.Pipe import Pipe

class Load:
    
    @classmethod
    def loadHDF(self, pipe):
        return pipe
