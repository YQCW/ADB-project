class Transaction(object):
    """
    This is the transaction class. Attributes:
    id: the unique id of transaction
    operations: the operations inside a transaction
    beginTime: the time stamp when the transaction begin
    willAbort: whether the transaction will abort at the commit time because of the site failure
    readOnly: whether the transaction is a read-only transaction
    """
    def __init__(self, id, beginTime, readOnly):
        self.id = id
        self.operations = []
        self.beginTime = beginTime
        self.willAbort = False
        self.readOnly = readOnly

    def abort(self):
        """
        change the willAbort tag to true:
        input: None
        output: None
        """
        self.willAbort = True

    def addOperation(self, operation):
        """
        add an operation to the transaction
        input: operation object
        output: None
        """
        self.operations.append(operation)
