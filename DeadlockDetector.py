class WaitForGraph(object):

    def __init__(self, tm):
        #the transaction manager
        self.tm = tm

        #transactions involved in a circle
        self.trace = []

        #key is the item, value is a list of transactions that access the item
        self.itemToOps = {}

        #key is the start point, value is the end point
        self.wait_for = {}


    def addOp(self, ops, op, id):
        ops.add(op)
        self.itemToOps[id] = ops

    def addWait(self, op, id):
        waits = self.wait_for.get(id, set())
        waits.add(op.para[0])
        self.wait_for[id] = waits

    def addOperation(self, operation):

        type = operation.type

        if type == "R" or type == "W":
            para = operation.para
            transactionId, itemId = para[0], para[1]

            if not self.tm.transactions[transactionId].readOnly:
                ops = self.itemToOps.get(itemId, set())

                if type == "W":
                    for op in ops:
                        if op.type == "W" and op.para[0] == transactionId:
                            addOp(ops, operation, itemId)
                            return

                    for op in ops:
                        if op.para[0] != transactionId:
                            addWait(op, transactionId)
                else:
                    for op in ops:
                        if op.para[0] == transactionId:
                            addOp(ops, operation, itemId)
                            return

                    for op in ops:
                        if op.type == "W" and op.para[0] != transactionId:
                            addWait(op, transactionId)

                addOp(ops, operation, itemId)

    def DFS(self, cur_node, target, visited):
        visited[cur_node] = True

        if cur_node in self.wait_for:
            self.trace.append(cur_node)
            neighbor_nodes = self.wait_for[cur_node]

            for neighbor in neighbor_nodes:
                if neighbor == target:
                    return True
                else:
                    if neighbor not in visited:
                        continue
                    if not visited[neighbor]:
                        if self.DFS(neighbor, target, visited):
                            return True
            self.trace.pop(-1)
        return False

    def checkDeadlock(self):
        self.trace = []
        nodes = list(self.wait_for.keys())
        for target in self.wait_for.keys():
            visited = {node: False for node in self.wait_for.keys()}
            if self.DFS(target, target, visited):
                return True
        return False

    def removeTransaction(self, transaction_id):
        for item, ops in self.itemToOps.items():
            tmp = {}
            for op in ops:
                if op.para[0] != transaction_id:
                    tmp.add(op)
            self.itemToOps[item] = tmp
        del self.wait_for[transaction_id]

    def getInvolvedTransactions(self):
        return self.trace
