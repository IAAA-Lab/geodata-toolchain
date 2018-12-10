# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Module: Decorators
# Description: This module contains any decorator used by the workflow

def run(func):
    print("Before exec")
    def wrapper_run():
        func()
    return wrapper_run
    
