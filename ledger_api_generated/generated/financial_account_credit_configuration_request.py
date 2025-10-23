# lithic-schemagen

from __future__ import annotations

from typing import Optional
from uuid import UUID

from ledger_api_generated.common import BaseResponse
from pydantic import Field, conint, constr


class FinancialAccountCreditConfigurationRequest(BaseResponse):
    credit_limit: Optional[conint(ge=0)] = None
    external_bank_account_token: Optional[UUID] = None
    credit_product_token: Optional[str] = Field(
        None, description='Globally unique identifier for the credit product'
    )
    tier: Optional[constr(min_length=1)] = Field(
        None, description='Tier to assign to a financial account'
    )
