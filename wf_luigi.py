from gdtc.gdtc_aux.db import Db
from gdtc.load.insert_hdf import InsertHDF
from gdtc.load.insert_shp import InsertSHP
from gdtc.transform.clip_hdf_with_shp_db import ClipHDFWithSHPDB
from gdtc.transform.remove_empty_rasters import RemoveEmptyRasters
from pipeline.targets.postgres_target import PostgresTarget

import luigi

class InsertHDFTask(luigi.Task):
    def requires(self):
        pass

    def output(self):
        return PostgresTarget(Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres'))

    def run(self):
        hdf_table = InsertHDF(
                file_name = 'MCD12Q1.A2006001.h17v04.006.2018054121935',
                layer = '1',
                db = self.output(),
                reproyect = True,
                srcSRS = '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs',
                dstSRS = 'EPSG:25830',
                cell_res = 1000
            ).run()
        
class InsertSHPTask(luigi.Task):
    def requires(self):
        return InsertHDFTask()
    
    def output(self):
        return PostgresTarget(Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres'))

    def run(self):
        shp_table = InsertSHP(
                file_name = 'Comunidades_Autonomas_ETRS89_30N',
                db = self.output(),
                coord_sys = '25830'
            ).run()

class ClipHDFWithSHPDBTask(luigi.Task):
    def requires(self):
        return InsertSHPTask()

    def output(self):
        return PostgresTarget(Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres'))

    def run(self):
        for gid in range(1, 20):
            ClipHDFWithSHPDB(
                hdf_table = 'hdf_table',
                shp_table = 'Comunidades_Autonomas_ETRS89_30N',
                db = self.output(),
                rid = 1,
                gid = gid,
                coord_sys = '25830',
                clean = False
            ).run()

class RemoveEmptyRastersTask(luigi.Task):
    def requires(self):
        return ClipHDFWithSHPDBTask()
    
    def output(self):
        return PostgresTarget(Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres'))

    def run(self):
        RemoveEmptyRasters(
            table = 'clips',
            db = self.output()
        ).run()

class Workflow(luigi.Task):
    def requires(self):
       pass

    def output(self):
        return luigi.LocalTarget('workflow_end.txt')

    def run(self):
        yield RemoveEmptyRastersTask()

        with open('workflow_end.txt', 'w+') as file:
            file.close()

if __name__ == '__main__':
    
    luigi.build([
        Workflow()
    ], local_scheduler=False, log_level="DEBUG")

    luigi.run()