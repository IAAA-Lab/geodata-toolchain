import os
import shutil

basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    LUIGI_BASE = os.environ.get('LUIGI_BASE') or basedir
    LUIGI_TMP = os.environ.get('LUIGI_TMP') or '{}\\tmp'.format(basedir)
    DEPENDENCIES_FILE = os.environ.get('DEPENDENCIES_FILE') or '{}\\dependencies.txt'.format(LUIGI_TMP)
    DEBUG = True
    CLEAN_ON_SUCCESS = True