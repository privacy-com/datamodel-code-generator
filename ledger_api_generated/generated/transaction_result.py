# lithic-schemagen

from __future__ import annotations

from enum import Enum


class TransactionResult(Enum):
    APPROVED = 'APPROVED'
    DECLINED = 'DECLINED'
    PENDING = 'PENDING'
