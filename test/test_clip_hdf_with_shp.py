import unittest
import psycopg2

from gdtc.transform.clip_hdf_with_shp import ClipHDFWithSHP
from gdtc.gdtc_aux.db import Db


class TestClipHDFWithSHP(unittest.TestCase):

    def test(self):
        expected = '''0100000000000000000000F03F000000000000F0BF00000000000000000000000000000000000000000000000000000000000000000000000000000000'''
        
        self.db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'geo-rasters', '30')
        
        ClipHDFWithSHP(
            hdf_file_name='MCD12Q1.A2006001.h17v04.006.2018054121935',
            layer='1',
            coord_sys='4269',
            shp_file_name='Comunidades_Autonomas_ETRS89_30N',
            db=self.db
        ).run()

        sql = ''' SELECT * FROM clips  '''
        
        with psycopg2.connect(
           port = self.db.port,
           database = self.db.database,
           user = self.db.user,
           password = self.db.password) as connection:
               connection.set_client_encoding('utf-8')
               self.db.connection = connection
               with self.db.connection.cursor() as cur:
                   cur.execute(sql)
                   results = cur.fetchall()

        self.assertEqual(results[0][0], expected)


if __name__ == '__main__':
    unittest.main()
