# lithic-schemagen

from __future__ import annotations

from enum import Enum


class TransactionStatus(Enum):
    PENDING = 'PENDING'
    SETTLED = 'SETTLED'
    DECLINED = 'DECLINED'
    REVERSED = 'REVERSED'
    CANCELED = 'CANCELED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'
