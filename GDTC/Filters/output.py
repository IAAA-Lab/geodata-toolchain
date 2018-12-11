# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Output Filters
# Description: This module encapsulates the output filters.
#              That means: Any filter related with putting information into the Exploitation DB

import numpy
import os
import subprocess
import psycopg2
from subprocess import call
from GDTC.Pipe import Pipe

def loadHDF(pipe, file_name, *params):
    """ This function loads an HDFfile from working db into the exploitation db specified in the pipe """
    
    pipe.error = True
    print('loadHDF: Mock execution incorrect')

    return pipe

def loadSHP(pipe, file_name, *params):
    """ This function loads an SHP file from working db into the exploitation db specified in the pipe """
    
    print('loadSHP: Mock execution correct')

    return pipe

def loadGeoPackage(pipe, file_name, *params):
    """ This function loads a GeoPackage file from working db into the exploitation db specified in the pipe """
    
    print('loadSHP: Mock execution correct')

    return pipe
