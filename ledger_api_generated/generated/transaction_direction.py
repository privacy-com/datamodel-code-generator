# lithic-schemagen

from __future__ import annotations

from enum import Enum


class TransactionDirection(Enum):
    CREDIT = 'CREDIT'
    DEBIT = 'DEBIT'
