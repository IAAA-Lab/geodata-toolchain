# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Stage
# Decription: This class is an abstraction to encapsulate a single bunch of filters to execute.
#             It lets the user define a task or step on it’s workflow. The user can inspect the
#             filters added to the stage and run them sequentially or concurrently. 

from GDTC.Pipe import Pipe
import asyncio

class Stage:
    """ This class lets the user define diferent stages on its workflow """

    def __init__(self, pipe):
        self.pipe = pipe
        self.to_run = [] # {'id': id, 'function': f, 'args': args}
        self.exec_log = []
        self.errors = [] # {'id': id}
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
        
        print('<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('\nInspection for object: {}:\n'.format(self))
        print('Functions to run:\n')

        # Print functions to run
        for f in self.to_run:
            print('<< {} >>'.format(f['id']))
            print('Function: {}'.format(f['function'].__name__))
            print('Description: {}'.format(f['function'].__doc__))
            print('Args: {}\n'.format(f['args']))

        print('Errors:\n')

        # Print errors
        for f in self.errors:
            print('<< {} >>'.format(f['id']))
            print('Function: {}'.format(f['function'].__name__))
        
        print('<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

        return self

    def run(self):
        """ This function runs the complete stage """

        # Run each function
        for f in self.to_run:
            item = {'id': f['id'], 'function': f['function']}
            
            self.pipe = f['function'](self.pipe, *f['args']) # pipe = f(pipe, args)

            # If not error on exec, remove function from list to_run and from error list if that was there
            if(self.pipe.error == False):
                self.to_run = self.to_run[1:len(self.to_run)]
                if item in self.errors:
                    self.errors = self.errors.remove(item)
            else:
                self.errors.append(item)

        return self
