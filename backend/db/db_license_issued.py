import time

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql

from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class License_issued(Base, Auto_repr):
    __tablename__ = "license_issued"
    license_id = Column(Integer, primary_key=True)
    org_id = Column(Integer)
    issued_date = Column(postgresql.BIGINT)
    license_type_id = Column(Integer)
    discount_id = Column(Integer)
