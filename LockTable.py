class LockTable(object):
    def __init__(self):
        """
        for each item in dictionary lockTable, key is the item id, value is lock type (0 for shared 1 for exclusive)
        and a list of transactions
        """
        self.lockTable = {}

    def addLock(self, itemId, transactionId, lockType):
        #several conditions to consider:
        #no lock on certain item
        #print("call addLock:",itemId,transactionId,lockType)
        if itemId not in self.lockTable.keys():
            #print(self.lockTable.keys())
            self.lockTable[itemId] = [lockType,[transactionId]]
            #print(self.lockTable)
            return True
        else:
            # existing shared lock, want shared lock
            if self.lockTable[itemId][0] == 0 and lockType == 0:
                if transactionId not in self.lockTable[itemId][1]:
                    self.lockTable[itemId][1].append(transactionId)
                return True
            # existing exclusive lock, want shared lock
            elif self.lockTable[itemId][0] == 1 and lockType == 0:
                if transactionId in self.lockTable[itemId][1]:
                    return True
                else:
                    return False
            #existing shared lock, want exclusive lock
            elif self.lockTable[itemId][0] == 0 and lockType == 1:
                if transactionId in self.lockTable[itemId][1] and len(self.lockTable[itemId][1]) == 1:
                    self.lockTable[itemId][0] = 1
                    self.lockTable[itemId][1] = [transactionId]
                    return True
                else:
                    return False
            #existing exclusive lock, want exclusive lock
            elif self.lockTable[itemId][0] == 1 and lockType == 1:
                if transactionId in self.lockTable[itemId][1]:
                    return True
                else:
                    return False

    def releaseLock(self,itemId, transactionId):
        #if the lock is exclusive lock
        if self.lockTable[itemId][0] == 1:
            if len(self.lockTable[itemId][1]) == 1 and transactionId in self.lockTable[itemId][1]:
                self.lockTable.pop(itemId)
                return True
        #else if the lock is shared lock
        else:
            if transactionId in self.lockTable[itemId][1]:
                self.lockTable[itemId][1].remove(transactionId)
                return True
        return False

    def clearLockTable(self):
        #call this function when a site fail and clear the lock table
        self.lockTable = {}

    def releaseTransactionLock(self,transactionId):
        #search through the lock table and release the lock hold by certain transaction
        #print("call release transaction lock: ",transactionId)
        toBeDeleted = []
        for itemId, value in self.lockTable.items():
            lockType, transactions = value
            if transactionId in transactions:
                transactions.remove(transactionId)
            if len(transactions) == 0:
                toBeDeleted.append(itemId)
        #print("tobedeleted:",toBeDeleted)
        for item in toBeDeleted:
            del self.lockTable[item]
        #print(self.lockTable)

    def getInvolvedTransactions(self):
        transactions = set()
        for itemId, locks in self.lockTable.items():
            # Transaction has read lock on var_id
            for transactionId in locks[1]:
                transactions.add(transactionId)
        return transactions
