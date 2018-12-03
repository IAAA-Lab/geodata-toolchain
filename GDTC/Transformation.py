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

class Transformation:
    
    @classmethod
    def convertSHP2HDF(cls, file_name, pipe):
        return pipe

    @classmethod
    def mergeHDFwithHDF(cls, file_name1, file_name2, pipe):
        return pipe

    @classmethod
    def clipHDFwithSHP(cls, hdf_file_name, shp_file_name, pipe):
        return pipe