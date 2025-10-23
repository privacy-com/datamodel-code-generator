# lithic-schemagen

from __future__ import annotations

from enum import Enum


class TransferMethod(Enum):
    ACH_NEXT_DAY = 'ACH_NEXT_DAY'
    ACH_SAME_DAY = 'ACH_SAME_DAY'
    WIRE = 'WIRE'
