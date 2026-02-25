import json
import time

from pandas.core.arrays import boolean
from sqlalchemy import Boolean, Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.schema import PrimaryKeyConstraint

from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Ds_meta(Base, Auto_repr):
    __tablename__ = "ds_meta"
    version_id = Column(Integer, ForeignKey("datasets_versions.version_id"))
    dataset_id = Column(Integer, ForeignKey("datasets_versions.dataset_id"))
    row_count = Column(Integer, nullable=False)
    col_count = Column(postgresql.BIGINT, nullable=False)
    col_info = Column(postgresql.JSON, nullable=False)
    ds_size = Column(Integer, nullable=False)
    is_deleted = Column(Boolean, default=False)
    PrimaryKeyConstraint(version_id, dataset_id)


def create_ds_meta(
    session: db_session,
    version_id: int,
    dataset_id: int,
    row_count: int,
    col_count: int,
    col_info: json,
    ds_size: int = -1,
    commit: bool = True,
) -> Dict[str, Any]:
    try:
        data_meta_ = Ds_meta()
        data_meta_.version_id = version_id
        data_meta_.dataset_id = dataset_id
        data_meta_.row_count = row_count
        data_meta_.col_count = col_count
        data_meta_.col_info = col_info
        data_meta_.ds_size = ds_size
        session.add(data_meta_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": data_meta_.dataset_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e


def read_ds_meta(
    session: db_session, dataset_id: int = None, version_id: int = None
) -> Dict[str, Any]:
    try:
        query = session.query(Ds_meta).filter(Ds_meta.is_deleted == False)
        if dataset_id is not None:
            query = query.filter(Ds_meta.dataset_id == dataset_id)
        if version_id is not None:
            query = query.filter(Ds_meta.version_id == version_id)
        ds_meta_list = []
        if query.count() != 0:
            for ds_meta in query:
                ds_meta_list.append(ds_meta.to_dict())
            return {"status": 1, "msg": ds_meta_list}
        else:
            return {"status": 0, "msg": "No meta data found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def delete_ds_meta(
    session: db_session, dataset_id: int = None, commit: bool = True
) -> Dict[str, Any]:
    try:
        query = session.query(Ds_meta).filter(Ds_meta.is_deleted == False)
        
        if dataset_id:
            query = query.filter(Ds_meta.dataset_id == dataset_id)
        
        if query.count() != 0:
            query.update({"is_deleted": True})
            if commit: 
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "DS meta deleted"}
        else:
            return {"status": 0, "msg": "No meta data found"}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e
