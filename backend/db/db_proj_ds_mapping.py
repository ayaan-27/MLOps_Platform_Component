from re import sub
import time

from sqlalchemy import Boolean, Column, ForeignKey, Integer, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_, or_

from db.db_datasets import Datasets
from db.db_datasets_versions import Dataset_version

from db.db_metadata import Auto_repr, Base
from db.db_projects import Projects
from db.db_proj_user import Project_user
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Proj_ds_mapping(Base, Auto_repr):
    __tablename__ = "proj_ds_mapping"
    pk = Column(Integer, primary_key=True)

    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.project_id"))
    creation_time = Column(postgresql.BIGINT, default=int(time.time()), nullable=False)
    creation_user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    public = Column(Boolean, default=False)
    is_deleted = Column(Boolean, default=False)
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))


def create_proj_ds_map(
    session: db_session,
    dataset_id: int,
    creation_user_id: int,
    project_id: int = None,
    public: Boolean = False,
    commit: Boolean = True,
) -> Dict[str, Any]:

    try:
        proj_ds_map_ = Proj_ds_mapping()
        proj_ds_map_.dataset_id = dataset_id
        proj_ds_map_.project_id = project_id
        proj_ds_map_.creation_user_id = creation_user_id
        proj_ds_map_.public = public
        proj_ds_map_.last_modified = int(time.time())
        session.add(proj_ds_map_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": proj_ds_map_.pk}
    except Exception as e:
        session.rollback()
        LOGGER.exception(e)
        raise (e)


def read_proj_ds_map(
    session: db_session,
    dataset_id: int = None,
    project_id: int = None,
    public: bool = None,
) -> Dict[str, Any]:
    try:
        query = session.query(Proj_ds_mapping).filter(Proj_ds_mapping.is_deleted == False)
        if public:
            query = query.filter(Proj_ds_mapping.public == public)
        if dataset_id:
            query = query.filter(Proj_ds_mapping.dataset_id == dataset_id)
        if project_id:
            query = query.filter(
                or_(
                    Proj_ds_mapping.project_id == project_id
                )
            )
        if query.count() != 0:
            proj_ds_list = []
            for i in query:
                proj_ds_list.append(i.to_dict())
            return {"status": 1, "msg": proj_ds_list}
        else:
            return {"status": 0, "msg": "No Dataset found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)

    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def del_proj_ds_map(
    session: db_session,
    dataset_id: int = None,
    project_id: int = None,
    commit: Boolean = True,
) -> Dict[str, Any]:
    try:
        query = session.query(Proj_ds_mapping).filter(
            and_(Proj_ds_mapping.is_deleted == False, Proj_ds_mapping.public == False)
        )
        if dataset_id:
            query = query.filter(Proj_ds_mapping.dataset_id == dataset_id)

        if project_id:
            query = query.filter(Proj_ds_mapping.project_id == project_id)

        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})

            if commit:
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "Dataset Project combination deleted"}
        else:
            return {"status": 0, "msg": "Dataset Project combination not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)

#! use this for datastore
def list_datasets_details(session: db_session, user_id: int = None, dataset_id: int = None, completed: bool = True) -> Dict[str, Any]:

    try:
        subq = session.query(Proj_ds_mapping.dataset_id).join(
            Project_user, Proj_ds_mapping.project_id == Project_user.project_id).filter(
                and_(Project_user.is_deleted == False, or_(Project_user.user_id == user_id, Proj_ds_mapping.public == True)))
        
        query = session.query(
            Datasets.name, Datasets.user_id, Datasets.creation_time.label("cration_time"), Datasets.dataset_id,\
                 func.max(Dataset_version.creation_time).label("last_modified")).join(
                     Dataset_version, Datasets.dataset_id == Dataset_version.dataset_id).filter(and_(Datasets.is_deleted == False,
                         Datasets.dataset_id.in_(subq))).group_by(Datasets.dataset_id)
        if dataset_id:
            query = query.filter(Datasets.dataset_id == dataset_id)
        
        if completed:
            query = query.filter(Datasets.completed == completed)
                    
        if query.count() != 0:
            dataset_list = []
            for i in query:
                dataset_info = {}
                dataset_info["name"] = i[0]
                dataset_info["user_id"] = i[1]
                dataset_info["creation_time"] = i[2]
                dataset_info["dataset_id"] = i[3]
                dataset_info["last_modified"] = i[4]
                dataset_list.append(dataset_info)
            return {"status": 1, "msg": dataset_list}
        else:
            return {"status": 1, "msg": "Dataset(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e
