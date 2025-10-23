# lithic-schemagen

from __future__ import annotations

from enum import Enum


class ManagementOperationDirection(Enum):
    CREDIT = 'CREDIT'
    DEBIT = 'DEBIT'
