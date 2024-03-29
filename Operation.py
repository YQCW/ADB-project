import re
from Transaction import Transaction

def parseOp(line):
    """
    Parse operation in one line
    input: one command in a  line, including the comment
    output: the operation type and parameters
    """
    regex = r'(.*)\((.*?)\)'
    lst = line.strip()
    if lst.startswith('//') or lst == "":
        return None
    else:
        print("command: "+lst)
        match = re.search(regex, lst)
        #print(match)
        op = match.group(1)
        para = match.group(2).split(',')
        for i in range(len(para)):
            para[i] = para[i].strip()
        return op, para

def readNotFromCopy(transactionId, itemId, site):
    """
    Read item from site without copy
    input: transaction id, item id, site object
    output: return true if read successfully
    """
    #if the item is modified by the transaction, just read from the log
    if transactionId in site.log and itemId in site.log[transactionId]:
        result = site.log[transactionId][itemId]
    else:
    #if not, read from the site data list
        result = site.data[itemId-1]

    print("x" + str(itemId) + ":" + str(result))

    return True

def getNumber(itemId):
    """
    get the item id from the string like "x12"
    input: item id with character 'x'
    output item id without character 'x'
    """
    return int(itemId[1:])

class Operation(object):
    def __init__(self, para):
        self.type = None
        self.para = para  
        self.transaction = None 

    def execute(self, time, tm, retry=False):
        pass

    def append(self, tm):
        """
        add the operation to the transaction list and wait for graph of the transaction manager
        input: transaction manager object
        output: None
        """
        tid = self.para[0]

        tm.transactions[tid].addOperation(self)
        tm.waitforGraph.addOperation(self)


class Begin(Operation):
    def __init__(self, para):
        self.para = para
        self.type = 'begin'
        self.transaction = para[0]

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        t = Transaction(self.para[0], time, False)
        if t.id not in tm.transactions:
            tm.transactions[t.id] = t
        return True


class BeginRO(Operation):
    def __init__(self, para):
        self.para = para
        self.type = 'beginRO'
        self.transaction = para[0]

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        t = Transaction(self.transaction, time, True)

        if t.id not in tm.transactions:
            for site in tm.sites:
                site.snapshot(time)
            tm.transactions[t.id] = t

        return True

class Fail(Operation):
    def __init__(self, para):
        self.para = para
        self.transaction = para[0]
        self.type = 'fail'

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        site_id = int(self.para[0])
        site = tm.sites[site_id-1]
        transactions = site.lockTable.getInvolvedTransactions()
        #print(transactions)
        # if a site fail and any item is accessed by a transaction before and the transaction hasn't commit or abort,
        # the transaction will abort at End command
        for tid in transactions:
            tm.transactions[tid].willAbort = True
        site.fail()
        return True


class Recover(Operation):
    def __init__(self, para):
        self.para = para
        self.transaction = para[0]
        self.type = 'recover'

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        site_id = int(self.para[0])
        tm.sites[site_id-1].up = True
        return True


class End(Operation):
    def __init__(self, para):
        self.para = para
        self.type = 'end'
        self.transaction = para[0]

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        if not retry:
            self.append(tm)

        if tm.transactions[self.transaction].willAbort:
            tm.abortTransaction(self.transaction)
            return True

        tid = self.transaction

        if tid in tm.blockedTransactions:
            return False

        print("Transaction "+tid+" commits")

        for site in tm.sites:
            if site.up:
                if tid in site.log:
                    for item_id, item in site.log[tid].items():
                        site.data[item_id-1] = item
                        site.accessible[item_id-1] = True
                    del site.log[tid]

            elif site.up:
                if tm.transactions[tid].beginTime in site.snapshots:
                    del site.snapshots[tm.transactions[tid].beginTime]

            site.lockTable.releaseTransactionLock(tid)

        if tid in tm.blockedTransactions:
            tm.transactions.remove(tid)
        
        tm.waitforGraph.removeTransaction(tid)

        return True

class R(Operation):
    def __init__(self, para):
        self.para = para
        self.type = 'R'
        self.transaction = para[0]

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        if retry == False:
            self.append(tm)
        transactionId = self.para[0]
        itemId = getNumber(self.para[1])
        # if the transaction is read-only
        if tm.transactions[transactionId].readOnly == True:
            transactionStartTime = tm.transactions[transactionId].beginTime
            if itemId % 2 != 0:
            #if the item is uniquely owned by one site
                site = tm.sites[itemId % 10]
                if site.up == False:
                #if the site is down, we just need to retry later
                    print("Transaction " + str(transactionId) + "for Site " + str(site.siteId) + " for site down")
                    return False
                elif transactionStartTime in site.snapshots.keys() and site.snapshots[transactionStartTime][itemId-1] != None:
                    print("x"+str(itemId)+":"+str(site.snapshots[transactionStartTime][itemId-1]))
                    return True
            else:
            #if the item is owned by every site
                canRead = False
                for site in tm.sites:
                    # we should verify this logic
                    # if the site is down, turn canRead to false, return false because we can retry later
                    if site.up == False and transactionStartTime in site.snapshots.keys() and site.snapshots[transactionStartTime][itemId-1] != None:
                        canRead = True
                    elif transactionStartTime in site.snapshots.keys() and site.snapshots[transactionStartTime][itemId-1] != None:
                        print("x"+str(itemId)+":"+str(site.snapshots[transactionStartTime][itemId-1]))
                        return True
                if canRead == True:
                    print("Transaction " + str(transactionId) + " waits for no data available")
                if canRead == False:
                    #if no site contains the item is up, just abort the transaction and no need to retry
                    tm.abortTransaction(transactionId)
                    #print("Transaction " + str(transactionId) + " aborts for no copies available")
                    #tm.transactions[transactionId].willAbort = True
                    return True
        #if the transaction is not read-only
        else:
            if itemId % 2 != 0:
            #if the item is uniquely owned by one site
                site = tm.sites[itemId % 10]
                if not site.up:
                    print("Transaction " + str(transactionId) + "for Site " + str(site.siteId) + " for site down")
                    return False
                elif site.accessible[itemId-1] == True:
                    if site.lockTable.addLock(itemId,transactionId,0):
                        result = readNotFromCopy(transactionId, itemId, site)
                        return result
                    else:
                        #cannot get lock, return false and try again later
                        print("Transaction " + str(transactionId) + " waits for Site " + str(site.siteId) + " for lock conflict")
                        return False
            else:
            #if the item is owned by every site
                for site in tm.sites:
                    if site.up == False:
                        #current site is down, try next site
                        continue
                    if site.accessible[itemId-1] == True:
                        if site.lockTable.addLock(itemId, transactionId, 0):
                            result = readNotFromCopy(transactionId, itemId, site)
                            return result
                if tm.sites[0].lockTable.addLock(itemId, transactionId, 0)==False:
                    print("Transaction " + str(transactionId) + " waits for conflict lock")
        return False


class W(Operation):
    def __init__(self, para):
        self.para = para
        self.transaction = para[0]
        self.type = 'W'

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        if retry == False:
            self.append(tm)
        transactionId = self.para[0]
        itemId = getNumber(self.para[1])
        newValue = int(self.para[2])

        #if the item is uniquely owned by one site
        if itemId % 2 != 0:
            site = tm.sites[itemId % 10]
            # the site is down, return false and retry later
            if not site.up:
                print("Transaction "+str(transactionId)+"for Site "+str(site.siteId)+" for site down")
                return False
            # the site is up and get the lock, write now
            elif site.lockTable.addLock(itemId,transactionId,1):
                logs = site.log.get(transactionId, {})
                logs[itemId] = newValue
                site.log[transactionId] = logs
                print("Transaction "+str(transactionId) + " write succeed. Affected sites:"+str(site.siteId))
                return True
            # the site is up but cannot get the lock, retry later
            else:
                print("Transaction "+str(transactionId)+" waits for Site "+str(site.siteId)+" for lock conflict")
                return False
        #if the item is owned by every site
        else:
            locks = []
            # try to lock all of the sites that have the item
            for site in tm.sites:
                # if the site fails, just continue, no need to wait for it
                if site.up == False:
                    continue
                # try to lock the item on the site and succeed
                elif site.lockTable.addLock(itemId,transactionId,1):
                    locks.append(site)
                # cannot get lock, release all the lock obtained before and retry later
                else:
                    for lockSite in locks:
                        lockSite.lockTable.releaseLock(itemId,transactionId)
                    print("Transaction " + str(transactionId) + " waits for all up sites for lock conflict")
                    return False
            #if any up site cannot get lock, retry later
            if len(locks) == 0:
                print("Transaction " + str(transactionId) + " waits for all up sites for lock conflict")
                return False

            # if the transaction has all of the locks, write to the log
            for site in locks:
                logs = site.log.get(transactionId, {})
                logs[itemId] = newValue
                site.log[transactionId] = logs
            print("Transaction "+str(transactionId) + " write succeed. Affected sites:",end="")
            for site in locks:
                print(str(site.siteId),end=" ")
            print("")
            return True

class Dump(Operation):
    def __init__(self, para):
        self.para = para
        self.type = 'dump'
        self.transaction = para[0]

    def execute(self, time, tm, retry=False):
        """
        execute the operation
        input: time stamp, transaction manager object, whether the execution is retry
        output: True if success, False if fail
        """
        for site in tm.sites:
            print("site "+str(site.siteId)+" - ",end="")
            data = site.data
            for i in range(20):
                if data[i] == None:
                    print("x" + str(i + 1) + ":None", end=" ")
                else:
                    print("x"+str(i+1)+":"+ str(data[i]), end=" ")
            print("\n")
        return True
