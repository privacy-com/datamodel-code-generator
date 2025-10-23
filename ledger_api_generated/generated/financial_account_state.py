# lithic-schemagen

from __future__ import annotations

from enum import Enum


class FinancialAccountState(Enum):
    PENDING = 'PENDING'
    CURRENT = 'CURRENT'
    DELINQUENT = 'DELINQUENT'
    CHARGED_OFF = 'CHARGED_OFF'
