from gdtc.load.insert_hdf import InsertHDF
from gdtc.load.insert_shp import InsertSHP
from gdtc.gdtc_aux.db import Db

from gdtc.gdtc_aux.config import Config

class Workflow():

    def __init__(self, db):
        self.db = db

    def run(self):
        InsertHDF(
            file_name='MCD12Q1.A2006001.h17v04.006.2018054121935',
            db=self.db,
            coord_sys="4269"
            ).run()

        InsertSHP(
            file_name='Comunidades_Autonomas_ETRS89_30N',
            db=self.db
            ).run()

if __name__ == '__main__':
    db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters', '30')
    Workflow(db).run()