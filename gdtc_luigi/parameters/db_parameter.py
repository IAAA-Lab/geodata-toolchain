# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Class: DBParameter
# Description: A DBParameter is a Parameter of DB typer which inherits from 
#              luigi Parameter

import luigi
from ..gdtc_base.db import Db

class DBParameter(luigi.Parameter):

    def parse(self, db):
        # Check if db is of type Db
        # assert(type(db) is Db), "The object is of type {}".format(type(db))
        
        # Get args
        args = db.__str__()[10:-1].split(',')

        # Check it has 7 args
        assert(len(args) == 7)

        # Return Db object
        return Db(args[0], args[1], args[2], args[3], args[4], args[5], args[6], )
