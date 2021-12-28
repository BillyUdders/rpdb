import uuid
from typing import NewType

from rpdb.operations import Operation, OperationType

TransactionID = NewType("TransactionID", uuid.UUID)


class Transaction(list):
    def __init__(self):
        super().__init__()
        self.id: TransactionID = TransactionID(uuid.uuid4())

    def do(self, op_type: OperationType, key=None, value=None):
        self.append(Operation(op_type, key, value))

    def __repr__(self) -> str:
        return f"Transaction(ops={self}, id={self.id})"
