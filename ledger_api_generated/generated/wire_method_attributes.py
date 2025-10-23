# lithic-schemagen

from __future__ import annotations

from typing import Optional
from uuid import UUID

from ledger_api_generated.common import BaseResponse
from pydantic import Field

from . import wire_transfer_type


class WireMethodAttributes(BaseResponse):
    wire_transfer_type: wire_transfer_type.WireTransferType = Field(
        ..., description='Type of wire transfer'
    )
    previous_transfer: Optional[UUID] = Field(
        None, description='UUID of previous transfer if this is a retry'
    )
    lithic_individual_name: Optional[str] = Field(
        None, description='Lithic individual name'
    )
    lithic_bank_name: Optional[str] = Field(None, description='Lithic bank name')
    lithic_bank_routing_number: Optional[str] = Field(
        None, description='Lithic bank routing number'
    )
    external_individual_name: Optional[str] = Field(
        None, description='External individual name'
    )
    external_bank_name: Optional[str] = Field(None, description='External bank name')
    external_bank_routing_number: Optional[str] = Field(
        None, description='External bank routing number'
    )
