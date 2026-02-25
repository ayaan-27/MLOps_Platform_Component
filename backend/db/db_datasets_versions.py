import time

from pandas.core.arrays import boolean
from sqlalchemy import Boolean, Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.expression import func
from sqlalchemy.sql.schema import PrimaryKeyConstraint

from db.db_datasets import Datasets
from db.db_metadata import Auto_repr, Base
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()

# TODO
# job_id foreign key
# profiling_job_id foreing key - sql


class Dataset_version(Base, Auto_repr):
    __tablename__ = "datasets_versions"
    version_id = Column(Integer)
    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id"))
    user_id = Column(Integer, ForeignKey("users.user_id"))
    creation_time = Column(Integer, nullable=False, default=int(time.time()))
    prev_id = Column(Integer, default=-1)  # Previous version_id
    location = Column(postgresql.VARCHAR(100), nullable=False)
    job_id = Column(Integer, default=-1)
    profiling_done = Column(Boolean, default=False)
    profiling_job_id = Column(Integer, default=-1)
    is_deleted = Column(Boolean, default=False)
    PrimaryKeyConstraint(version_id, dataset_id)


def create_datasets_versions(
    session: db_session,
    dataset_id: int,
    user_id: int,
    location: str,
    prev_id: int = -1,
    commit: bool = True,
) -> Dict[str, Any]:
    try:
        data_ver_ = Dataset_version()
        data_ver_.dataset_id = dataset_id
        data_ver_.user_id = user_id
        data_ver_.prev_id = prev_id
        data_ver_.location = location
        data_ver_.creation_time = int(time.time())
        data_ver_.version_id = get_max_ds_ver_id(session, dataset_id)["msg"] + 1
        session.add(data_ver_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": data_ver_.version_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e


def get_max_ds_ver_id(session: db_session, dataset_id: int) -> Dict[str, Any]:
    try:
        query = session.query(func.max(Dataset_version.version_id)).filter(
            Dataset_version.dataset_id == dataset_id
        )
        if query.scalar() is not None:
            max_ver_id = query.scalar()
            return {"status": 1, "msg": max_ver_id}
        else:
            # -1 so that + 1 makes it 0
            return {"status": 1, "msg": -1}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def update_profiling_details(
    session: db_session,
    dataset_id: int,
    version_id: int,
    profiling_done: Boolean,
    profiling_job_id: int = -1,
) -> Dict[str, Any]:
    try:
        query = session.query(Dataset_version).filter(
            and_(
                Dataset_version.version_id == version_id,
                Dataset_version.dataset_id == dataset_id,
            )
        )
        if query.count() != 0:
            temp = query.first()
            temp.profiling_done = profiling_done
            temp.profiling_job_id = profiling_job_id
            session.commit()
            return {"status": 1, "msg": version_id}
        else:
            return {"status": 0, "msg": "dataset version not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def read_datasets_versions(
    session: db_session,
    dataset_id: int = None,
    version_id: int = None,
    user_id: int = None,
    job_id: int = None,
    prev_id: int = None,
    completed: Boolean = True,
) -> Dict[str, Any]:
    try:
        subq = session.query(Datasets.dataset_id).filter_by(
            completed=completed, is_deleted=False
        )
        query = session.query(Dataset_version).filter_by(is_deleted=False)
        query = query.filter(Dataset_version.dataset_id.in_(subq))

        if dataset_id is not None:
            query = query.filter(Dataset_version.dataset_id == dataset_id)
        if version_id is not None:
            query = query.filter(Dataset_version.version_id == version_id)
        if user_id is not None:
            query = query.filter(Dataset_version.user_id == user_id)
        if job_id is not None:
            query = query.filter(Dataset_version.job_id == job_id)
        if prev_id is not None:
            query = query.filter(Dataset_version.prev_id == prev_id)

        dataset_version_list = []
        if query.count() != 0:
            for dataset_ver in query:
                dataset_version_list.append(dataset_ver.to_dict())
            return {"status": 1, "msg": dataset_version_list}
        else:
            return {"status": 0, "msg": "dataset version not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def delete_datasets_versions(
    session: db_session, dataset_id: int, version_id: int = None, commit = True
) -> Dict[str, Any]:
    try:
        query = session.query(Dataset_version).filter(and_(
            Dataset_version.dataset_id == dataset_id, Dataset_version.is_deleted == False
            )
        )
        if version_id is not None:
            query = query.filter(Dataset_version.version_id == version_id)

        if query.count() != 0:
            query.update({"is_deleted": True})
            if commit:
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "Dataset versions deleted"}
        else:
            return {"status": 0, "msg": "dataset version not found"}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e

def list_datasets(session: db_session, user_id: int = None, dataset_id: int = None, completed: bool = True) -> Dict[str, Any]:

    try:
        query = session.query(Datasets)
        
        if completed:
            query = query.filter(Datasets.user_id == user_id)

        if completed:
            query = query.filter(Datasets.completed == completed)
        
        if dataset_id is not None:
            query = query.filter(Datasets.dataset_id == dataset_id)
                    
        if query.count() != 0:
            dataset_list = []
            for i in query:
                dataset_list.append(i.to_dict())
            return {"status": 1, "msg": dataset_list}
        else:
            return {"status": 1, "msg": "Dataset(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e