from gdtc.load.insert_hdf import InsertHDF
from gdtc.load.insert_shp import InsertSHP
from gdtc.transform.clip_hdf_with_shp_db import ClipHDFWithSHPDB
from gdtc.transform.remove_empty_rasters import RemoveEmptyRasters
from gdtc.gdtc_aux.db import Db

from gdtc.gdtc_aux.config import Config

class Workflow():

    def __init__(self, db):
        self.db = db

    def run(self):

        hdf_table = InsertHDF(
                        file_name = 'MCD12Q1.A2006001.h17v04.006.2018054121935',
                        layer = '1',
                        db = self.db,
                        reproyect = True,
                        srcSRS = '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs',
                        dstSRS = 'EPSG:25830',
                        cell_res = 1000
                    ).run()

        shp_table = InsertSHP(
                        file_name = 'Comunidades_Autonomas_ETRS89_30N',
                        db = self.db,
                        coord_sys = '25830'
                    ).run()

        for gid in range(1, 20):
            ClipHDFWithSHPDB(
                hdf_table = hdf_table,
                shp_table = shp_table,
                db = self.db,
                rid = 1,
                gid = gid,
                coord_sys = '25830',
                clean = False
            ).run()
        
        RemoveEmptyRasters(
            table = 'clips',
            db = self.db
        ).run()

if __name__ == '__main__':
    db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres')
    Workflow(db).run()