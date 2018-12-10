# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Stage

from GDTC.Pipe import Pipe
import asyncio

class Stage:
    """ This class lets the user define diferent stages on its workflow """

    def __init__(self, db):
        self.pipe = Pipe(db)
        self.to_run = []
        self.exec_log = []
        self.errors = []
        self.params = []
        self.id = 0

    def add(self, f, *args):
        """ This functions adds a filter to the stage """
        
        self.id+=1
        self.to_run.append({'id': self.id, 'function': f, 'args': args})

        return self

    def configure(self, param):
        """ This function adds a configuration parameter to the stage """

        self.params.append(param)

        return self

    def inspect(self):
        """ This function lets the inspection of the stage """
        
        print('\nInspection for object: {}:\n'.format(self))
        print('Functions to run:\n')

        for f in self.to_run:
            print('<< {} >>'.format(f['id']))
            print('Function: {}'.format(f['function'].__name__))
            print('Description: {}'.format(f['function'].__doc__))
            print('Args: {}\n'.format(f['args']))
        
        return self

    def run(self):
        """ This function runs the complete stage """

        for f in self.to_run:
            self.pipe = f['function'](self.pipe, *f['args'])
            self.to_run = self.to_run[:len(self.to_run)-1]

        return self
