import time

from sqlalchemy import Boolean, Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_

from db.db_metadata import Auto_repr, Base
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Datasets(Base, Auto_repr):
    __tablename__ = "datasets"
    dataset_id = Column(Integer, primary_key=True)
    name = Column(postgresql.VARCHAR(50), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    ds_desc = Column(postgresql.VARCHAR(500), nullable=False)
    creation_time = Column(postgresql.BIGINT, default=int(time.time()))
    is_deleted = Column(Boolean, default=False)
    completed = Column(Boolean, default=False)
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))


def create_dataset(
    session: db_session,
    name: str,
    user_id: int,
    ds_desc: str,
    completed: bool = False,
    commit: bool = True,
) -> Dict[str, Any]:
    try:
        dataset_ = Datasets()
        dataset_.user_id = user_id
        dataset_.name = name
        dataset_.ds_desc = ds_desc
        dataset_.completed = completed
        session.add(dataset_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": dataset_.dataset_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e


def update_visibility(
    session: db_session, dataset_id: int, completed: bool
) -> Dict[str, Any]:
    try:
        query = session.query(Datasets).filter(Datasets.dataset_id == dataset_id)
        if query.count() != 0:
            temp = query.first()
            temp.completed = completed
            session.commit()
            return {"status": 1, "msg": dataset_id}
        else:
            return {"status": 0, "msg": "dataset not found"}

    except Exception as e:
        LOGGER.exception(e)
        raise e


def read_datasets(
    session: db_session,
    user_id: int = None,
    dataset_id: int = None,
    completed: bool = True,
) -> Dict[str, Any]:
    try:
        query = session.query(Datasets).filter(Datasets.is_deleted == False)
        if user_id:
            query = query.filter(Datasets.user_id == user_id)
        if dataset_id:
            query = query.filter(Datasets.dataset_id == dataset_id)
        if completed:
            query = query.filter(Datasets.completed == completed)
        dataset_list = []
        if query.count() != 0:
            for dataset in query:
                dataset_list.append(dataset.to_dict())
            return {"status": 1, "msg": dataset_list}
        else:
            return {"status": 0, "msg": "dataset(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def delete_datasets(session: db_session, dataset_id: int, commit = True) -> Dict[str, Any]:
    try:
        query = session.query(Datasets).filter(
            and_(
                Datasets.dataset_id == dataset_id, Datasets.is_deleted == False, Datasets.completed == True
                )
            )
        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "Dataset Deleted"}
        else:
            return {"status": 0, "msg": "Dataset not found"}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e
