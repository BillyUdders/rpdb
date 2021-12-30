import uuid
from typing import List, NewType

from attrs import define

from rpdb.operations import Write, WriterOps

TransactionID = NewType("TransactionID", uuid.UUID)


@define
class Transaction:
    id: TransactionID = TransactionID(uuid.uuid4())
    operations: List[Write] = []

    def do(self, op_type: WriterOps, key=None, value=None):
        self.operations.append(Write(op_type, key, value))
