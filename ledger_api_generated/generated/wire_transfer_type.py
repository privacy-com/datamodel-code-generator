# lithic-schemagen

from __future__ import annotations

from enum import Enum


class WireTransferType(Enum):
    FEDWIRE = 'FEDWIRE'
    SWIFT = 'SWIFT'
