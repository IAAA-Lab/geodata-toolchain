import os

class Config(object):
    BASEDIR = os.environ.get('BASE_DIR') or os.path.abspath(os.path.dirname(__file__))
    LUIGI_TMP = os.environ.get('BASE_TMP') or '{}\\tmp'.format(BASEDIR)
    DEPENDENCIES_FILE = os.environ.get('DEPENDENCIES_FILE') or '{}\\dependencies.txt'.format(BASEDIR)
    DEBUG = True
    CLEAN_ON_SUCCESS = True
    