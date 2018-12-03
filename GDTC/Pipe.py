import time

class Pipe:

    def __init__(self, db_src):
        self.db_src = db_src
        self.init_timestamp = time.time()
        self.last_timestamp = self.init_timestamp
        self.state = 0
        self.result = None

    def updateTimestamp(self):
        self.last_timestamp = time.time()

    def setDB_src(self, db_src):
        self.db_src = db_src
        self.updateTimestamp()

    def setDB_dst(self, db_dst):
        self.db_dst = db_dst
        self.updateTimestamp()

    def setResult(self, result):
        self.result = result
        self.updateTimestamp()

    def getResult(self):
        self.updateTimestamp()
        return self.result

    def getDB_src(self):
        self.updateTimestamp()
        return self.db_src

    def getDB_dst(self):
        self.updateTimestamp()
        return self.db_dst
