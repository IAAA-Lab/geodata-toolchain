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

class ClipHDFWithSHPDB():
    """
    Clip HDF layer with SHP vector db
    """

    def __init__(self, hdf_table, shp_table, db, rid, gid, coord_sys, clean=True):
        self.hdf_table = hdf_table
        self.shp_table = shp_table
        self.db = db
        self.rid = rid
        self.gid = gid
        self.coord_sys = coord_sys
        self.clean = clean

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
                        shp_table=self.shp_table,
                        hdf_table=self.hdf_table,
                        rid=rid,
                        gid=gid
                        )

        return sql

    def run(self):
        
        if self.clean:
            self.db.executeQuery(''' DROP TABLE IF EXISTS clips ''')

        # Clip HDF with SHP
        self.db.executeQuery(self.buildSQLQuery('geom_{coord}'.format(coord=self.coord_sys), self.rid, self.gid))
