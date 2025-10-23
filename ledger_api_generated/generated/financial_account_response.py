# lithic-schemagen

from __future__ import annotations

from typing import Optional, Union
from uuid import UUID

from ledger_api_generated.common import BaseResponse
from pendulum import DateTime
from pydantic import Field

from . import (
    account_financial_account_type,
    financial_account_credit_config,
    financial_account_status,
    financial_account_substatus,
    instance_financial_account_type,
)


class FinancialAccountResponse(BaseResponse):
    token: UUID = Field(
        ...,
        description='Globally unique identifier for the account',
        example='b68b7424-aa69-4cbc-a946-30d90181b621',
    )
    created: DateTime
    updated: DateTime
    type: Union[
        instance_financial_account_type.InstanceFinancialAccountType,
        account_financial_account_type.AccountFinancialAccountType,
    ]
    routing_number: Optional[str] = None
    account_number: Optional[str] = None
    nickname: Optional[str]
    account_token: Optional[UUID]
    status: financial_account_status.FinancialAccountStatus
    substatus: Optional[financial_account_substatus.FinancialAccountSubstatus] = None
    is_for_benefit_of: bool = Field(
        ...,
        description='Whether financial account is for the benefit of another entity',
    )
    credit_configuration: Optional[
        financial_account_credit_config.FinancialAccountCreditConfig
    ]
