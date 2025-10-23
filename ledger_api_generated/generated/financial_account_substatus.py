# lithic-schemagen

from __future__ import annotations

from enum import Enum


class FinancialAccountSubstatus(Enum):
    CHARGED_OFF_DELINQUENT = 'CHARGED_OFF_DELINQUENT'
    CHARGED_OFF_FRAUD = 'CHARGED_OFF_FRAUD'
    END_USER_REQUEST = 'END_USER_REQUEST'
    BANK_REQUEST = 'BANK_REQUEST'
    DELINQUENT = 'DELINQUENT'
