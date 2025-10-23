# lithic-schemagen

from __future__ import annotations

from typing import List, Optional

from ledger_api_generated.common import BaseResponse
from pydantic import Field, conint

from . import ach_sec_code


class AchMethodAttributes(BaseResponse):
    sec_code: ach_sec_code.AchSecCode = Field(
        ..., description='SEC code for ACH transaction'
    )
    return_reason_code: Optional[str] = Field(
        None, description='Return reason code if the transaction was returned'
    )
    retries: Optional[conint(ge=0)] = Field(
        None, description='Number of retries attempted'
    )
    company_id: Optional[str] = Field(
        None, description='Company ID for the ACH transaction'
    )
    receipt_routing_number: Optional[str] = Field(
        None, description='Receipt routing number'
    )
    trace_numbers: Optional[List[str]] = Field(
        [], description='Trace numbers for the ACH transaction'
    )
    addenda: Optional[str] = Field(None, description='Addenda information')
