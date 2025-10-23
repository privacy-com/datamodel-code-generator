# lithic-schemagen

from __future__ import annotations

from typing import Optional
from uuid import UUID

from ledger_api_generated.common import BaseResponse
from pydantic import Field

from . import charged_off_reason, financial_account_state


class FinancialAccountCreditConfig(BaseResponse):
    credit_limit: Optional[int]
    external_bank_account_token: Optional[UUID]
    credit_product_token: Optional[str] = Field(
        ..., description='Globally unique identifier for the credit product'
    )
    tier: Optional[str] = Field(
        ..., description='Tier assigned to the financial account'
    )
    is_spend_blocked: bool
    financial_account_state: Optional[financial_account_state.FinancialAccountState]
    charged_off_reason: Optional[charged_off_reason.ChargedOffReason]
