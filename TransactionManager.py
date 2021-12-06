from DeadlockDetector import WaitForGraph

class TransactionManager(object):
    def __init__(self):
        self.sites = []
        self.transactions = {}
        self.waitforGraph = WaitForGraph(self)
        self.blockedTransactions = []
        self.blockedOperations = []

    def step(self, operation, timeStamp):
        #retry the blocked transactions/operations and update the blocked transactions/operations
        newBlockedOperations = []
        newBlockedTransactions = []
        for op in self.blockedOperations:
            result = op.execute(timeStamp, self, retry=True)
            if result == False:
                newBlockedOperations.append(op)
                if op.type == "end" and op.transaction not in newBlockedTransactions:
                    continue
                else:
                    newBlockedTransactions.append(op.transaction)
        self.blockedTransactions = newBlockedTransactions
        self.blockedOperations = newBlockedOperations

        #execute new transactions/operations
        result = operation.execute(timeStamp, self, retry=False)
        if result == False:
            self.blockedOperations.append(operation)

        #check the deadlock and abort if existing
        if operation.type in {"R", "W"} and self.waitforGraph.checkDeadlock():
            involvedTransactions = self.waitforGraph.getInvolvedTransactions()
            t = self.getYoungest(involvedTransactions)
            self.abortTransaction(t)

    def getYoungest(self,transactions):
        maxTime = self.transactions[transactions[0]].beginTime
        youngest = transactions[0]
        for transaction in transactions:
            if self.transactions[transaction].beginTime > maxTime:
                youngest = transaction
                maxTime = self.transactions[youngest].beginTime
        return youngest

    def abortTransaction(self,transactionId):
        for site in self.sites:
            if site.up == True:
                # release locks hold by transaction in every up site
                site.lockTable.releaseTransactionLock(transactionId)
                #reverse changes (delete logs) of transaction
                site.abortTransaction(transactionId)
        #remove the operations of transaction in block operations of transactionManager
        toBeDeleted = []
        for op in self.blockedOperations:
            if op.transaction == transactionId:
                toBeDeleted.append(op)
        for i in toBeDeleted:
            self.blockedOperations.remove(i)

        #remove the transaction in transactionManager
        del self.transactions[transactionId]

        #remove the transaction in deadlock detector
        self.waitforGraph.removeTransaction(transactionId)

        #remove the transaction in block transaction of transactionManager
        if transactionId in self.blockedTransactions:
            self.blockedTransactions.remove(transactionId)

        #print the abort information
        print(transactionId+" aborts")
