from datetime import datetime
from enum import Enum
from typing import List
from pydantic import BaseModel


class GroupPeriod(Enum):
    hour = 'hour'
    day = 'day'
    week = 'week'
    month = 'month'


class IncomeRequest(BaseModel):
    dt_from: datetime
    dt_upto: datetime
    group_type: GroupPeriod


class OutcomeResult(BaseModel):
    dataset: List[int]
    labels: List[str]
