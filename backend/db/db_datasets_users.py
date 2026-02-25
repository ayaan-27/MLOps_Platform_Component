import time

from sqlalchemy import Boolean, Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query, relationship

from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Datasets_users(Base, Auto_repr):
    __tablename__ = "datasets_users"
    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id"), primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), primary_key=True)
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    is_deleted = Column(Boolean, default=False)


def create_datasets_user(
    session: db_session,
    dataset_id: str,
    user_id: str,
    creation_user_id: str,
    commit: Boolean = True,
) -> Dict[str, Any]:
    try:
        data_user_ = Datasets_users()
        data_user_.dataset_id = dataset_id
        data_user_.user_id = user_id
        data_user_.creation_user_id = creation_user_id
        session.add(data_user_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": data_user_.dataset_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e


def read_datasets_user(
    session: db_session, dataset_id: int = None, user_id: int = None
) -> Dict[str, Any]:
    try:
        query = session.query(Datasets_users).filter(Datasets_users.is_deleted == False)
        if dataset_id is not None:
            query = query.filter(Datasets_users.dataset_id == dataset_id)
        if user_id is not None:
            query = query.filter(Datasets_users.user_id == user_id)
        datasets_user_list = []
        if query.count() != 0:
            for dataset in query:
                datasets_user_list.append(dataset.to_dict())
            return {"status": 1, "msg": datasets_user_list}
        else:
            return {"status": 0, "msg": "datasets User combination not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def delete_dataset_user(
    session: db_session, dataset_id: str, user_id: str
) -> Dict[str, Any]:
    try:
        query = session.query(Datasets_users).filter(
            and_(
                Datasets_users.dataset_id == dataset_id, Datasets_users.user_id == user_id
            )
        )
        if query.count() != 0:
            temp = query.first()
            temp.is_deleted = True
            session.commit()
            return {"status": 1, "msg": dataset_id}
        else:
            return {"status": 0, "msg": "datasets User combination not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e
