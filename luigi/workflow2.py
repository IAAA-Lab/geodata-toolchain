import luigi
from PostgresTarget import PostgresTarget
from DB import Db
from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call

# Generate Tiff file
class GenerateTiffFile(luigi.Task):
    """
    Generates tiff file
    """
    in_file_name = luigi.Parameter()
    layer_num = luigi.Parameter()

    task_namespace = 'transform'

    def requires(self):
        pass
    
    def output(self):
        out_file_name = '{}.tif'.format(str(self.in_file_name).rsplit('.', 1)[0])
        return luigi.LocalTarget(out_file_name)
    
    def run(self):
        # Load file
        hdf = gdal.Open(self.in_file_name, gdal.GA_ReadOnly)
        layer = gdal.Open(hdf.GetSubDatasets()[int(self.layer_num)][0], gdal.GA_ReadOnly)
        layer_array = layer.ReadAsArray()

        out_file_name = '{}.tif'.format(str(self.in_file_name).rsplit('.', 1)[0])

        # Generate intermediate file in tiff format
        out = gdal.GetDriverByName('GTiff').Create(out_file_name, layer.RasterXSize, layer.RasterYSize, 1, gdal.GDT_Byte, ['COMPRESS=LZW', 'TILED=YES'])
        out.SetGeoTransform(layer.GetGeoTransform())
        out.SetProjection(layer.GetProjection())
        out.GetRasterBand(1).WriteArray(layer_array)
        
        # Write file to disk
        out = None


# Define Task for inserting HDF file into PostgreSQL DB
class insertHDF(luigi.Task):
    """
    Inserts an HDF file into the target Db
    """

    task_namespace = 'input'

    def requires(self):
        # Requires intermediate file in tiff format
        return GenerateTiffFile(in_file_name='MCD12Q1.A2006001.h17v04.006.2018054121935.hdf', layer_num=1)

    def output(self):
        db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'test', '14')
        return PostgresTarget(db)
    
    def run(self):
        sql = """INSERT INTO test (col) VALUES (101); SELECT * FROM test;"""
        print(self.output().connect().executeQuery(sql).result)

# Run workflow
if __name__ == '__main__':
    luigi.run(['input.insertHDF', '--workers', '1', '--local-scheduler'])
