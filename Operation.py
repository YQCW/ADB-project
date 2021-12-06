import re
from TransactionManager import TransactionManager
from Transaction import Transaction

def parseOp(line):
	regex = r'(.*)\((.*?)\)'
	lst = line.split()
	if lst[0] == '//':
		return None
	else:
		match = re.findall(regex, lst[0])
		op = match.group(1)
		para = match.group(2).split(',')
        for p in para:
            p.strip()
        return op, para

def readNotFromCopy(transactionId, itemId, site):
    #if the item is modified by the transaction, just read from the log
    if transactionId in site.log and itemId in site.log[transactionId]:
        result = site.log[transactionId][itemId]
    else:
    #if not, read from the site data list
        result = site.data[itemId-1]

    print("x" + itemId + ":" + site.siteId)

    return True

def getNumber(itemId):
    return int(itemId[1:])

class Operation(object):
    def __init__(self, para):
        self.type = None 
        self.para = para  
        self.transaction = None 

    def execute(self, time, tm, retry=False):
        pass

    def append(self, tm):
        tid = self.para[0]

        tm.transactions[tid].addOperation(self)
        tm.waitforGraph.addOperation(self)


class Begin(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'begin'

    def execute(self, time, tm, retry=False):
        t = Transaction(self.para[0], time)
        if t.id not in tm.transactions:
        	tm.transactions[t.id] = t
        return True


class BeginRO(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'beginRO'

    def execute(self, time, tm, retry=False):
        t = Transaction(self.para[0], time, True)

        if t.id not in tm.transactions:
            tm.transactions[t.id] = t
            for site in tm.sites:
                site.snapshot(time)

        return True

class Fail(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'fail'

    def execute(self, time, tm, retry=False):
        site_id = int(self.para[0])
        site = tm.sites[site_id-1]
        transactions = site.lockTable.get_involved_transactions()

        for tid in transactions:
            tm.transactions[tid].willAbort = True
        site.fail()
        return True


class Recover(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'recover'

    def execute(self, time, tm, retry=False):
        site_id = int(self.para[0])
        tm.sites[site_id-1].up = True
        return True


class End(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'end'

    def execute(self, time, tm, retry=False):
        if not retry:
            self.append(tm)

        if tm.transactions[self.para[0]].willAbort:
            tm.abort(self.para[0])
            return True

        tid = self.para[0]
        start_time = tm.transactions[tid].time

        if tid in tm.blockedTransactions:
            return False

        print("Transaction "+tid+" commits")

        for site in tm.sites:
            if site.up and tid in site.log:
                l = site.log[tid]
                for var_id, val in l.items():
                    site.data[var_id-1] = val
                    site.accessible[var_id - 1] = True
             
                site.log.pop(tid)

            elif site.up and start_time in site.snapshots:
                site.snapshots.pop(start_time)

            site.lockTable.releaseTransactionLocks(tid)

        if tid in tm.blockedTransactions:
            tm.transactions.remove(tid)
        
        tm.wait_for_graph.removeTransaction(tid)

        return True

class R(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'R'

    def execute(self, time, tm, retry=False):
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
                    return False
                elif transactionStartTime in site.snapshots.keys() and site.snapshots[transactionStartTime][itemId-1] != None:
                    print("x"+itemId+":"+site.siteId)
                    return True
            else:
            #if the item is owned by every site
                canRead = False
                for site in tm.sites:
                    # we should verify this logic
                    # if the site is down, turn canRead to false, return false because we can retry later
                    if site.up == False and transactionStartTime in site.snapshots and site.snapshots[transactionStartTime][itemId] != None:
                        canRead = True
                    elif transactionStartTime in site.snapshots and site.snapshots[transactionStartTime][itemId] != None:
                        print("x" + itemId + ":" + site.siteId)
                        return True
                if canRead == False:
                    #if no site contains the item is up, just abort the transaction and no need to retry
                    tm.abort(transactionId)
                    return True
                    
        #if the transaction is not read-only
        else:
            if itemId % 2 != 0:
            #if the item is uniquely owned by one site
                site = tm.sites[itemId % 10]
                if not site.up:
                    return False
                elif site.accesible[itemId-1] == True:
                    if site.lockTable.addLock(itemId,transactionId,0):
                        result = readNotFromCopy(transactionId, itemId, site)
                        return result
                    else:
                        #cannot get lock, return false and try again later
                        return False
            else:
            #if the item is owned by every site
                for site in tm.sites:
                    if site.up == False:
                        #current site is down, try next site
                        continue
                    if site.accesible[itemId-1] == True:
                        if site.lock_manager.try_lock_variable(transactionId, itemId, 0):
                            result = readNotFromCopy(transactionId, itemId, site)
                            return result
        return False


class W(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'W'

    def execute(self, time, tm, retry=False):
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
                return False
            # the site is up and get the lock, write now
            elif site.lockTable.addLockaddLock(itemId,transactionId,1):
                logs = site.log.get(transactionId, {})
                logs[itemId] = newValue
                site.log[transactionId] = logs
                return True
            # the site is up but cannot get the lock, retry later
            else:
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
                elif site.lockTable.addLockaddLock(itemId,transactionId,1):
                    locks.append(site)
                # cannot get lock, release all the lock obtained before and retry later
                else:
                    for site in locks:
                        site.lockTable.releaseLock(itemId,transactionId)
                    return False
            #if any up site cannot get lock, retry later
            if len(locks) == 0:
                return False

            # if the transaction has all of the locks, write to the log
            for site in locks:
                logs = site.log.get(transactionId, {})
                logs[transactionId] = newValue
                site.log[transactionId] = logs
            return True

class Dump(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'dump'

    def execute(self, time, tm, retry=False):
        for site in tm.sites:
            print("site "+site.siteId+" - ",end="")
            data = site.data
            for i in range(20):
                if data[i] == None:
                    print("x" + str(i + 1) + ": None", end="")
                else:
                    print("x"+str(i+1)+": "+ data[i], end="")
            print("\n")
        return True
