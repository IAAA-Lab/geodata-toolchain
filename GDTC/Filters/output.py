# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Filter.Load

#from osgeo import gdal
#from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call
from GDTC.Pipe import Pipe

def loadHDF(pipe, file_name, *params):
    """ This function loads an HDF file into the pipe """
    
    print('loadHDF: Mock execution correct')

    return pipe

def loadSHP(pipe, file_name, *params):
    """ This function loads an HDF file into the pipe """
    
    print('loadSHP: Mock execution correct')

    return pipe
