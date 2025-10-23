# lithic-schemagen

from __future__ import annotations

from enum import Enum


class Family(Enum):
    CARD = 'CARD'
    PAYMENT = 'PAYMENT'
    TRANSFER = 'TRANSFER'
    FINANCIAL = 'FINANCIAL'
    BOOK_TRANSFER = 'BOOK_TRANSFER'
    EXTERNAL_PAYMENT = 'EXTERNAL_PAYMENT'
    MANAGEMENT_OPERATION = 'MANAGEMENT_OPERATION'
