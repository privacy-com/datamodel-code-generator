# lithic-schemagen

from __future__ import annotations

from typing import List

from pydantic import Field

from . import (
    family,
    financial_transaction_event,
    transaction_category,
    transaction_result,
    transaction_status,
)
from .unified_base_transaction import UnifiedBaseTransaction


class FinancialTransaction(UnifiedBaseTransaction):
    family: family.Family = Field(
        'FINANCIAL', const=True, description='The family of the transaction'
    )
    category: transaction_category.TransactionCategory = Field(
        ..., description='Transaction category'
    )
    status: transaction_status.TransactionStatus = Field(
        ..., description='Transaction status'
    )
    result: transaction_result.TransactionResult = Field(
        ..., description='Transaction result'
    )
    settled_amount: int = Field(..., description='Settled amount in cents')
    pending_amount: int = Field(..., description='Pending amount in cents')
    events: List[financial_transaction_event.FinancialTransactionEvent] = Field(
        ..., description='List of transaction events'
    )
    descriptor: str = Field(..., description='Transaction descriptor')
