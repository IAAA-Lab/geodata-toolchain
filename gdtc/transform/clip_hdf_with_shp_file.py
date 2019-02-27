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

class ClipHDFWithSHPFile():
    """
    Clip HDF layer with SHP vector db
    """

    def __init__(self, hdf_file_name, layer, shp_coord_sys, shp_file_name, db, rid, gid, cell_res=None, reproyect=False, srcSRS=None, dstSRS=None, clean=False):
        self.hdf_file_name = hdf_file_name
        self.layer = layer
        self.shp_file_name = shp_file_name
        self.shp_coord_sys = shp_coord_sys
        self.db = db
        self.rid = rid
        self.gid = gid
        self.cell_res = cell_res
        self.reproyect = reproyect
        self.srcSRS = srcSRS
        self.dstSRS = dstSRS
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
        # Insert HDF
        self.hdf_table = InsertHDF(
                                file_name=self.hdf_file_name,
                                reproyect=self.reproyect,
                                cell_res=self.cell_res,
                                srcSRS=self.srcSRS,
                                dstSRS=self.dstSRS,
                                layer=self.layer,
                                db=self.db
                                ).run()

        # Insert SHP
        self.shp_table = InsertSHP(
                                file_name=self.shp_file_name,
                                db=self.db,
                                coord_sys=self.shp_coord_sys
                                ).run()
        
        if self.clean:
            self.db.executeQuery(''' DROP TABLE IF EXISTS clips ''')

        # Clip HDF with SHP
        self.db.executeQuery(self.buildSQLQuery('geom_{coord}'.format(coord=self.shp_coord_sys), self.rid, self.gid))
