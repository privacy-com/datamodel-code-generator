# lithic-schemagen

from __future__ import annotations

from enum import Enum


class TransactionSource(Enum):
    LITHIC = 'LITHIC'
    EXTERNAL = 'EXTERNAL'
    CUSTOMER = 'CUSTOMER'
