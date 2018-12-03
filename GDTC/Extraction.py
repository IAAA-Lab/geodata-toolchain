# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Extraction

from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call
from GDTC.Pipe import Pipe

class Extraction:
    
    @classmethod
    def insertHDF(cls, file_name, layer_num, params, pipe):
        """ This functions gets a layer from an hdf file and inserts it to db indicated in pipe"""

        # Load file
        hdf = gdal.Open(file_name, gdal.GA_ReadOnly)
        layer = gdal.Open(hdf.GetSubDatasets()[int(layer_num)][0], gdal.GA_ReadOnly)
        layer_array = layer.ReadAsArray()

        layer_path = os.path.join('./', os.path.basename(file_name + '.tif'))

        # Generate intermediate file in tiff format
        out = gdal.GetDriverByName('GTiff').Create(layer_path, layer.RasterXSize, layer.RasterYSize, 1, gdal.GDT_Byte, ['COMPRESS=LZW', 'TILED=YES'])
        out.SetGeoTransform(layer.GetGeoTransform())
        out.SetProjection(layer.GetProjection())
        out.GetRasterBand(1).WriteArray(layer_array)
        
        # Write file to disk
        out = None

        # Generate sql file
        cmd = 'raster2pgsql -I -C -s 4326 {} -F {} {} > {}.sql'.format(layer_path, pipe.getDB_src().table, params, file_name)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)

        # Insert into PostgreSQL/Postgis db
        file = open(file_name + '.sql', "r")

        pipe.getDB_src().connectDB()
        pipe.getDB_src().execQuery(file.read())
        pipe.getDB_src().closeDB()
        file.close()

        # Clean temp files
        os.remove(file_name + '.tif')
        os.remove(file_name + ".sql")

        return pipe

    @classmethod
    def insertSHP(cls, file_name, params, pipe):
        """ This function gets an shp file and inserts it to db """

        # Generate sql file
        cmd = 'shp2pgsql -s 4269 -g geom_4269 -I -W \"latin1\" {} \"{}\" > {}.sql'.format(params, file_name, file_name)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)

        # Insert into PostgreSQL/Postgis db
        file = open(file_name + '.sql', "r")
        
        pipe.getDB_src().connectDB()
        pipe.getDB_src().execQuery(file.read())
        pipe.getDB_src().closeDB()
        file.close()
        
        # Clean temp files
        os.remove(file_name + ".sql")

        return pipe

    @classmethod
    def insertGeoPackage(cls, file_name, pipe):
        """ This function gets a GeoPackage files and inserts it into db """
        return pipe

    @classmethod
    def getHDF(cls, file_name, pipe):
        """ This function gets an hdf file from db """

        # Query
        query = "SELECT * FROM \"{}\" WHERE filename='{}'".format(pipe.getDB_src.table, file_name)

        # Get from DB
        pipe.getDB_src().connectDB()
        hdfFile = pipe.getDB_src().execQuery(query)
        pipe.setResult(hdfFile)
        pipe.getDB_src().closeDB()

        return pipe
        