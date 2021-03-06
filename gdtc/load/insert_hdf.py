import numpy
import os
import subprocess
import psycopg2

from osgeo import gdal
from osgeo import ogr

from subprocess import call

from ..conversion.hdf2tif import HDF2TIF
from ..conversion.tif2sql import TIF2SQL
from ..load.exec_sql import ExecSQL
from ..gdtc_aux.db import Db

class InsertHDF():
    ''' Inserts a layer of an HDF file into PostgreSQL Db. The layer can be reproyected. '''

    def __init__(self, file_name, layer, db, table='hdf_table', reproyect=False, srcSRS=None, dstSRS=None, cell_res=None):
        self.file_name = file_name
        self.layer = layer
        self.db = db
        self.table = table
        self.reproyect = reproyect
        self.srcSRS = srcSRS
        self.dstSRS = dstSRS
        self.cell_res = cell_res

    def run(self):

        HDF2TIF(
            file_name = '{file_name}'.format(file_name=self.file_name),
            layer_num = self.layer,
            reproyect = self.reproyect,
            srcSRS = self.srcSRS,
            dstSRS = self.dstSRS,
            cell_res = self.cell_res
        ).run()

        junk, coord_sys = str(self.dstSRS).split(':')

        TIF2SQL(
            file_name='{file_name}'.format(file_name=self.file_name),
            coord_sys=coord_sys,
            db=self.db,
            table=self.table,
            layer_path='{file_name}'.format(file_name=self.file_name)
        ).run()

        ExecSQL(
            file_name='{file_name}'.format(file_name=self.file_name),
            db=self.db
        ).run()

        return self.table