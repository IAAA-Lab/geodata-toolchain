import numpy
import os
import subprocess
import psycopg2

from osgeo import gdal
from osgeo import ogr

from subprocess import call

from ..load.exec_sql import ExecSQL
from ..load.insert_hdf import InsertHDF
from ..load.insert_shp import InsertSHP

class ClipHDFWithSHP():
    """
    Insert SHP file into postgis db
    """

    def __init__(self, hdf_file_name, layer, coord_sys, shp_file_name, db):
        self.hdf_file_name = hdf_file_name
        self.shp_file_name = shp_file_name
        self.layer = layer
        self.coord_sys = coord_sys
        self.db = db

    def buildSQLQuery(self, geom, rid, gid):
        sql = '''
                CREATE TABLE IF NOT EXISTS clips (clip raster);
                INSERT INTO clips (clip) VALUES ((
                    SELECT ST_Clip (rast, 
                        (SELECT {geom} FROM {shp_table} WHERE gid = {gid})
                    , true)
                    AS clip
                FROM "{hdf_table}"
                WHERE rid = {rid}))

                '''.format(
                        geom=geom,
                        shp_table=self.shp_db.table,
                        hdf_table=self.hdf_db.table,
                        rid=rid,
                        gid=gid
                        )

        return sql

    def run(self):
        # Insert HDF
        self.hdf_db = InsertHDF(
                        file_name=self.hdf_file_name,
                        coord_sys=self.coord_sys,
                        layer=self.layer,
                        db=self.db
                        ).run()

        # Insert SHP
        self.shp_db = InsertSHP(
                        file_name=self.shp_file_name,
                        db=self.db
                        ).run()

        # Clip HDF with SHP
        self.db.executeQuery(self.buildSQLQuery('geom_4269', 1, 2))
