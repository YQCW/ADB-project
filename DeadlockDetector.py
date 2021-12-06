class WaitForGraph(object):

    def __init__(self, tm):
        #the transaction manager
        self.tm = tm

        #key is the item, value is a list of transactions that access the item
        self.itemToOps = {}

        #key is the start point, value is the end point
        self.wait_for = {}

        #transactions involved in a circle
        self.trace = []

    def addOperation(self, operation):
        """
        Add operation to self.itemToOps dictionary, for example, if the operation want to access x1,
        we add the operation in this way self.itemToOps["x1"].add(operation)

        Add new node in wait-for graph if the transactions does not exist

        ReadOnly operation will be ignored

        :param operation: Operation object
        :return: None
        """
        type = operation.type
        para = operation.para

        # Only add read and write operations
        if not (type == "R" or type == "W"):
            return

        transactionId, itemId = para[0], para[1]

        # ignore the operation belongs to a readonly transaction
        if self.tm.transactions[transactionId].readOnly:
            return

        # Get all operations on the variable
        ops = self.itemToOps.get(itemId, set())

        # Case 1: operation is R
        if type == "R":
            # Check if previous operation of the same transaction operated on the same variable
            # if so, no deadlock will be formed by adding this operation
            for op in ops:
                if op.para[0] == transactionId:
                    # Add operation to the dictionary
                    ops.add(operation)
                    self.itemToOps[itemId] = ops
                    return

            # for any operation which is on the same variable,
            # if op is W and transaction id is different, then there should be a edge
            # For example, op is W(T1, x1, 10), the operation to be added is R(T2, x1)
            # then the edge is T2 -> T1
            for op in ops:
                if op.type == "W" and op.para[0] != transactionId:
                    waits = self.wait_for.get(transactionId, set())
                    waits.add(op.para[0])
                    self.wait_for[transactionId] = waits
        # Case 2: operation is W
        else:
            # Check if previous operation of the same transaction operated on the same variable
            # if so, no deadlock will be formed by adding this operation
            for op in ops:
                if op.para[0] == transactionId and op.type == "W":
                    # Add operation to the dictionary
                    ops.add(operation)
                    self.itemToOps[itemId] = ops
                    return

            # W operation will conflict with all other operation on the same variable
            for op in ops:
                if op.para[0] != transactionId:
                    waits = self.wait_for.get(transactionId, set())
                    waits.add(op.para[0])
                    self.wait_for[transactionId] = waits

        # Add operation to the dictionary
        ops.add(operation)
        self.itemToOps[itemId] = ops

    def _recursive_check(self, cur_node, target, visited, trace):
        visited[cur_node] = True

        if cur_node not in self.wait_for:
            return False

        trace.append(cur_node)
        neighbor_nodes = self.wait_for[cur_node]

        for neighbor in neighbor_nodes:
            if neighbor == target:
                return True
            elif neighbor not in visited:
                continue
            elif not visited[neighbor]:
                if self._recursive_check(neighbor, target, visited, trace):
                    return True

        trace.pop(-1)
        return False

    def checkDeadlock(self):
        """
        Detect if there is a circle in current execution

        :return: True if there is a deadlock, otherwise False
        """
        nodes = list(self.wait_for.keys())
        self.trace = []

        for target in nodes:
            visited = {node: False for node in nodes}
            if self._recursive_check(target, target, visited, self.trace):
                return True
        return False

    def removeTransaction(self, transaction_id):
        """
        Remove wait-for node has the transaction_id, remove all operations belong to the transaction

        Typically, this function will be called when a transaction has been aborted or has committed

        :param transaction_id: identifier of the transaction
        :return: None
        """
        # Modify var_to_trans
        for var, ops in self.itemToOps.items():
            ops = {op for op in ops if op.para[0] != transaction_id}
            self.itemToOps[var] = ops

        # Modify wait for graph, delete the node of given transaction id
        self.wait_for.pop(transaction_id, None)

    def getInvolvedTransactions(self):
        return self.trace
