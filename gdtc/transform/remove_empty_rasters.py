import psycopg2

from ..load.exec_sql import ExecSQL

class RemoveEmptyRasters():
    """
    Clean empty rasters from hdf_table
    """

    def __init__(self, table, db):
        self.table = table
        self.db = db
        self.empty_raster = '0100000000000000000000F03F000000000000F0BF00000000000000000000000000000000000000000000000000000000000000000000000000000000'
   
    def run(self):

        sql = 'DELETE FROM {table} WHERE clip = \'{empty_raster}\''.format(table = self.table, empty_raster = self.empty_raster)
        
        # Clean empty rasters from hdf_table
        self.db.executeQuery(sql)
