# lithic-schemagen

from __future__ import annotations

from uuid import UUID

from ledger_api_generated.common import BaseResponse

from . import relationship_type


class InternalManagementOperationRelationship(BaseResponse):
    relationship_type: relationship_type.RelationshipType
    related_transaction_token: UUID
    related_transaction_event_token: UUID
    related_transaction_aggregate_token: UUID
