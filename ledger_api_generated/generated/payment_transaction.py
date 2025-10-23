# lithic-schemagen

from __future__ import annotations

from typing import List, Optional, Union
from uuid import UUID

from pendulum import Date
from pydantic import Field

from . import (
    ach_method_attributes,
    family,
    payment_transaction_event,
    transaction_category,
    transaction_direction,
    transaction_result,
    transaction_source,
    transfer_method,
    wire_method_attributes,
)
from .unified_base_transaction import UnifiedBaseTransaction


class PaymentTransaction(UnifiedBaseTransaction):
    family: family.Family = Field(
        'PAYMENT', const=True, description='The family of the transaction'
    )
    category: transaction_category.TransactionCategory = Field(
        ..., description='Transaction category'
    )
    result: transaction_result.TransactionResult = Field(
        ..., description='Transaction result'
    )
    method_attributes: Union[
        ach_method_attributes.AchMethodAttributes,
        wire_method_attributes.WireMethodAttributes,
    ] = Field(..., description='Method-specific attributes')
    financial_account_token: UUID = Field(..., description='Financial account token')
    external_bank_account_token: Optional[UUID] = Field(
        None, description='External bank account token'
    )
    direction: transaction_direction.TransactionDirection = Field(
        ..., description='Transfer direction'
    )
    source: transaction_source.TransactionSource = Field(
        ..., description='Transaction source'
    )
    method: transfer_method.TransferMethod = Field(..., description='Transfer method')
    settled_amount: int = Field(..., description='Settled amount in cents')
    pending_amount: int = Field(..., description='Pending amount in cents')
    events: List[payment_transaction_event.PaymentTransactionEvent] = Field(
        ..., description='List of transaction events'
    )
    descriptor: str = Field(..., description='Transaction descriptor')
    user_defined_id: Optional[str] = Field(None, description='User-defined identifier')
    expected_release_date: Optional[Date] = Field(
        None, description='Expected release date for the transaction'
    )
