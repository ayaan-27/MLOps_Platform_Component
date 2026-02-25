from typing import List

from sqlalchemy import Boolean, Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql

from db.db_datasets import Datasets
from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Pii(Base, Auto_repr):
    __tablename__ = "pii"
    pii_id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id"), nullable=False)
    pii_columns = Column(postgresql.VARCHAR(100))
    is_deleted = Column(Boolean, default=False)


def create_pii(sess: db_session, dataset_id: int, pii_columns: List) -> Dict[str, Any]:
    try:
        pii_ = Pii()
        pii_.dataset_id = dataset_id
        pii_.pii_columns = pii_columns
        sess.add(pii_)
        sess.commit()
        return {"status": 1, "msg": pii_.pii_id}
    except Exception as e:
        LOGGER.exception(e)
        raise e
