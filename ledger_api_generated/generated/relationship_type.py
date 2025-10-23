# lithic-schemagen

from __future__ import annotations

from enum import Enum


class RelationshipType(Enum):
    UNKNOWN = 'UNKNOWN'
    FEE = 'FEE'
    DISPUTE = 'DISPUTE'
