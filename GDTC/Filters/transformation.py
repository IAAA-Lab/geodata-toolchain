# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Transformation Filters
# Description: This module encapsulates the transformation filters.
#              That means: Any filter related with transforming information from the
#              Working DB and putting it back to the working db

from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call

def convertSHP2HDF(pipe, file_name, *params):
    """ This function converts an SPH file from db specified on pipe to an HDF file and stores it into the db specified on pipe """
        
    print('convertSHP2HDF: Mock execution correct')

    return pipe

def mergeHDFwithHDF(pipe, file_name1, file_name2, *params):
    """ This functions merges two HDF files from db specified on pipe and stores it into db specified on pipe """
            
    print('mergeHDFwithHDF: Mock execution correct')

    return pipe

def clipHDFwithSHP(pipe, hdf_file_name, shp_file_name, *params):
    """ This function clips an HDF file with SHP boundaries, both from db specified on pipe and stores the result into that db """
            
    print('clipHDFwithSHP: Mock execution correct')

    return pipe

def clipHDFwithCoordiantes(pipe, hdf_file_name, coord, *params):
    """ This function clips an HDF file from db specified on pipe with boundaries specified by coordinates and stores the result into the db specified on pipe """
            
    print('clipHDFwithCoordiantes: Mock execution correct')

    return pipe
    