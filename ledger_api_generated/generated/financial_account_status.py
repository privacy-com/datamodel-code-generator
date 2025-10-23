# lithic-schemagen

from __future__ import annotations

from enum import Enum


class FinancialAccountStatus(Enum):
    OPEN = 'OPEN'
    CLOSED = 'CLOSED'
    SUSPENDED = 'SUSPENDED'
    PENDING = 'PENDING'
