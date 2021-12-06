from LockTable import LockTable

#the index of site starts from 1
class site(object):
    def __init__(self, siteId):
        self.siteId = siteId
        self.lockTable = LockTable()
        self.data = [None] * 20
        self.accessible = []
        self.up = True
        self.snapshots = {}
        for i in range(1, 21):
            if i % 2 == 0 or i % 10 + 1 == siteId:
                self.data[i - 1] = 10 * i
                self.accessible.append(True)
            else:
                self.accessible.append(False)
        #log. key is the trasaction id and the value is the modifications, and the modifications
        #is also a dictionary, where key is item id and the value is new value)
        self.log = {}

    def fail(self):
        self.up = False
        self.log = {}
        self.lockTable.clearLockTable()
        for i in range(1,21):
            if i % 2 != 0 and i % 10 + 1 == self.site_id:
                self.accessible[i - 1] = True
            else:
                self.accessible[i - 1] = False

    def commit(self, transactionId):
        if transactionId in self.log.keys():
            log = self.log[transactionId]
            for key, value in log:
                self.data[key-1] = value
                if self.accessible[key-1] == False:
                    self.accessible = True
            del self.log[transactionId]
            return True
        else:
            return False

    def abortTransaction(self,transactionId):
        if transactionId in self.log.keys():
            del self.log[transactionId]

    def recover(self):
        self.up = True


    def snapshot(self, timeStamp):
        tmp = [None] * 20
        for i in range(1,21):
            if self.accessible[i-1] == True:
                tmp[i-1] = self.data[i-1]
        self.snapshots[timeStamp] = tmp.copy()

    def getItemFromSnapshot(self, timeStamp, index):
        return self.snapshots[timeStamp][index-1]
