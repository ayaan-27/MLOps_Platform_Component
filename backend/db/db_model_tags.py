import time

from sqlalchemy import Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_

from db.db_metadata import Auto_repr, Base
from db.db_roles import Roles
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Model_tags(Base, Auto_repr):
    __tablename__ = "model_tags"
    user_id = Column(Integer, ForeignKey("users.user_id"), primary_key=True)
    role_id = Column(Integer, ForeignKey("roles.role_id"), primary_key=True)
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    creation_time = Column(
        postgresql.BIGINT, nullable=False, default=int(time.time()), primary_key=True
    )
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))