# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Stage

from GDTC.Pipe import Pipe

class Stage:
    """ This class lets the user define diferent stages on its workflow """

    def __init__(self, db):
        self.pipe = Pipe(db)
        self.to_run = []

    def add(self, f, *args):
        """ This functions adds a filter to the stage """
        
        self.to_run.append({'function': f, 'args': args})
        return self

    def inspect(self):
        """ This function lets the inspection of the stage """

        i = 1
        
        print('\nInspection for object: {}:\n'.format(self))
        print('Functions to run:\n')

        for f in self.to_run:
            print('<< {} >>'.format(i))
            print('Function: {}'.format(f['function'].__name__))
            print('Description: {}'.format(f['function'].__doc__))
            print('Args: {}\n'.format(f['args']))
            i+=1
        
        return self

    def run(self):
        """ This function runs the complete stage """

        for f in self.to_run:
            f['function'](pipe=self.pipe, *f['args'])

        