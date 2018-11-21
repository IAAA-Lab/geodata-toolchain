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

class Extraction():

    def insertHDF(self, file_name, layer_num, db):
        """ This functions gets a layer from an hdf file and inserts it to db """

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
        cmd = 'raster2pgsql -I -C -a -s 4326 ' + layer_path + ' -F ' + db.table + ' > ' + file_name + '.sql'
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)

        # Insert into PostgreSQL/Postgis db
        file = open(file_name + '.sql', "r")
        db.execQuery(file.read())
        file.close()

        # Clean temp files
        os.remove(file_name + '.tif')
        os.remove(file_name + ".sql")
        