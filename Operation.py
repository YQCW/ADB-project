import re

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
        tm.wait_for_graph.addOperation(self)


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

class Dump(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'dump'

    def execute(self, time, tm, retry=False):
        print('dump')
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

    def execute(self, tick: int, tm, retry=False):
        site_id = int(self.para[0])
        tm.sites[site_id-1].up = True
        return True


class End(Operation):
    def __init__(self, para):
        super().__init__(para)
        self.type = 'end'

    def execute(self, tick: int, tm, retry=False):
        if not retry:
            self.append(tm)

        if tm.transactions[self.para[0]].willAbort:
            tm.abort(self.para[0])
            return True

        tid = self.para[0]
        start_time = tm.transactions[tid].time

        if tid in tm.blockedTransactions:
            return False

        print(f"Transaction {tid} commit")

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