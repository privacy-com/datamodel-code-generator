# lithic-schemagen

from __future__ import annotations

from enum import Enum


class ExternalResourceType(Enum):
    STATEMENT = 'STATEMENT'
    COLLECTION = 'COLLECTION'
    DISPUTE = 'DISPUTE'
    UNKNOWN = 'UNKNOWN'
