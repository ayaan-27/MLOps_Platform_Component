import json
import time

from pandas.core.arrays import boolean
from sqlalchemy import Boolean, Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query


from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()

class Auto_ml(Base, Auto_repr):
    __tablename__ = "auto_ml"
    auto_ml_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs_info.job_id"))
    auto_ml_type = Column(postgresql.VARCHAR(50), nullable=False)
    leadboard_loc = Column(postgresql.VARCHAR(50), nullable=False)
    is_deleted = Column(Boolean, default=False)

def create_auto_ml(
    session: db_session,
    job_id: int,
    auto_ml_type: str,
    leadboard_loc: str,
    commit: bool = True,
) -> Dict[str, Any]:
    try:
        automl_ = Auto_ml()
        automl_.job_id = job_id
        automl_.auto_ml_type = auto_ml_type
        automl_.leadboard_loc = leadboard_loc
        session.add(automl_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": automl_.auto_ml_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e