# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Filter.Transformation

from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call

def convertSHP2HDF(file_name, pipe):
    """ This function converts an SHP file into an HDF file """
    return pipe

def mergeHDFwithHDF(file_name1, file_name2, pipe):
    """ This function merges two HDF files """
    return pipe

def clipHDFwithSHP(hdf_file_name, shp_file_name, pipe):
    """ This function clips an HDF file with an SHP file """
    return pipe

def clipHDFwithCoordiantes(hdf_file_name, coord, pipe):
    """ This function clips an HDF file with the given coordinates """
    return pipe