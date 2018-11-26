# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Transformation

from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call

class Transformation():
    
    def convertSHP2HDF(self, file_name, db_src, db_dst):
        pass
    
    def mergeHDFwithHDF(self, file_name1, file_name2, db_src, db_dst):
        pass

    def clipHDFwithSHP(self, hdf_file_name, shp_file_name, db_src, db_dst):
        pass