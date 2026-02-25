from email.policy import default
import time
from enum import IntEnum

from sqlalchemy import Boolean, Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_

from db.db_datasets import Datasets
from db.db_datasets_versions import Dataset_version
from db.db_metadata import Auto_repr, Base
from db.db_proj_ds_mapping import Proj_ds_mapping, read_proj_ds_map
from db.db_proj_user import Project_user
from db.db_projects import Projects
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Pdvu_mapping(Base, Auto_repr):
    __tablename__ = "pdvu_mapping"
    pk = Column(Integer, primary_key=True)
    dataset_id = Column(
        Integer, ForeignKey("datasets_versions.dataset_id"), nullable=False
    )
    version_id = Column(
        Integer, ForeignKey("datasets_versions.version_id"), nullable=False
    )
    project_user_id = Column(Integer, ForeignKey("project_user.pk"), nullable=False)
    project_ds_id = Column(Integer, ForeignKey("proj_ds_mapping.pk"), nullable=False)
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    target_col = Column(postgresql.VARCHAR(100), default="")
    is_current = Column(postgresql.BOOLEAN, nullable=True, default=True)


def create_pdvu_map(
    sess: db_session,
    version_id: int,
    dataset_id: int,
    project_user_id: int,
    project_ds_id: int,
    target_col:str = "",
    commit: bool = True,
) -> Dict[str, Any]:

    try:
        # setting is_current of all the user project combo to False
        query = sess.query(Pdvu_mapping).filter(
            Pdvu_mapping.project_user_id == project_user_id
        )
        if query.count() != 0:
            for i in query:
                i.is_current = False

        pdvu_map_ = Pdvu_mapping()
        pdvu_map_.version_id = version_id
        pdvu_map_.dataset_id = dataset_id
        pdvu_map_.project_user_id = project_user_id
        pdvu_map_.project_ds_id = project_ds_id
        pdvu_map_.target_col = target_col
        sess.add(pdvu_map_)
        if commit:
            sess.commit()
        else:
            sess.flush()
        return {"status": 1, "msg": "Project Dataset Version User Mapped"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def current_update(
    sess: db_session, project_user_id: int, current: bool = False, commit: bool = True
) -> Dict[str, Any]:
    try:
        query = sess.query(Pdvu_mapping).filter(
            Pdvu_mapping.project_user_id == project_user_id
        )
        if query.count() != 0:
            for i in query:
                i.is_current = current
                i.last_modified = int(time.time())
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "Current project changed"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def read_pdvu_map(
    sess: db_session,
    pdvu_id: int = None,
    dataset_id: int = None,
    version_id: int = None,
    user_id: int = None,
    project_id: int = None,
    current: bool = True,
) -> Dict[str, Any]:
    try:

        query = sess.query(
            Pdvu_mapping.pk,
            Pdvu_mapping.dataset_id,
            Pdvu_mapping.version_id,
            Project_user.project_id,
            Project_user.user_id,
            Pdvu_mapping.creation_time,
        ).join(Project_user, Pdvu_mapping.project_user_id == Project_user.pk)

        if current:
            query = query.filter(Pdvu_mapping.is_current)

        if pdvu_id is not None:
            query = query.filter(Pdvu_mapping.pk == pdvu_id)

        if dataset_id is not None and version_id is None:
            query = query.filter(Pdvu_mapping.dataset_id == dataset_id)

        if dataset_id is not None and version_id is not None:
            query = query.filter(
                and_(
                    Pdvu_mapping.dataset_id == dataset_id,
                    Pdvu_mapping.version_id == version_id,
                )
            )

        if user_id is not None:
            query = query.filter(Project_user.user_id == user_id)

        if project_id is not None:
            query = query.filter(Project_user.project_id == project_id)

        if query.count() != 0:
            pdvu_list = []
            for i in query:
                pdvu = {}
                pdvu["pk"] = i[0]
                pdvu["dataset_id"] = i[1]
                pdvu["version_id"] = i[2]
                pdvu["project_id"] = i[3]
                pdvu["user_id"] = i[4]
                pdvu["creation_time"] = i[5]
                pdvu_list.append(pdvu)
            return {"status": 1, "msg": pdvu_list}
        else:
            return {"status": 0, "msg": "Project Dataset Version User mapping not found"}

    except Exception as e:
        LOGGER.exception(e)
        raise (e)

def list_datasets_versions(
    session: db_session,
    dataset_id: int = None,
    version_id: int = None,
    user_id: int = None,
) -> Dict[str, Any]:
    try:
        query = session.query(Datasets.dataset_id, Datasets.name, Dataset_version.version_id, Dataset_version.user_id, \
            Dataset_version.creation_time).join(Datasets, Dataset_version.dataset_id == Datasets.dataset_id).filter(
                Dataset_version.is_deleted == False)


        if dataset_id:
            query = query.filter(Dataset_version.dataset_id == dataset_id)
        if version_id:
            query = query.filter(Dataset_version.version_id == version_id)
        if user_id:
            query = query.filter(Dataset_version.user_id == user_id)
    
        if query.count() != 0:
            dataset_version_list = []
            for i in query:
                dataset_ver = {}
                dataset_ver["dataset_id"] = i[0]
                dataset_ver["name"] = i[1]
                dataset_ver["version_id"] = i[2]
                dataset_ver["user_id"] = i[3]
                dataset_ver["creation_time"] = i[4]
                dataset_ver["created_in_project"] = session.query(Project_user.project_id).join(
                    Pdvu_mapping, Project_user.pk == Pdvu_mapping.project_user_id).filter(and_(
                        Pdvu_mapping.dataset_id == i[0], Pdvu_mapping.version_id == i[2])).first()[0]
                dataset_version_list.append(dataset_ver)
            return {"status": 1, "msg": dataset_version_list}
        else:
            return {"status": 0, "msg": "dataset version not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e

def read_pdvu_id(
    session: db_session,
    project_id: int,
    dataset_id: int,
    version_id: int,
    user_id: int
) -> Dict[str, Any]:
    try:
        query_proj_user_pk = session.query(Project_user.pk).filter(and_(Project_user.project_id == project_id, \
            Project_user.user_id == user_id))
        query_proj_ds_pk = session.query(Proj_ds_mapping.pk).filter(and_(Proj_ds_mapping.dataset_id == dataset_id, \
            Proj_ds_mapping.project_id == project_id))
        
        query_pdvu_id = session.query(Pdvu_mapping.pk).filter(and_(Pdvu_mapping.dataset_id == dataset_id, \
            Pdvu_mapping.version_id == version_id, Pdvu_mapping.project_user_id == query_proj_user_pk[0][0], \
                Pdvu_mapping.project_ds_id == query_proj_ds_pk[0][0]))
        
        if query_pdvu_id.count() != 0:
            return {"status": 1, "msg": query_pdvu_id.all()[0][0]}
        else:
            {"status": 0, "msg": "Pdvu ID not found"}

    except Exception as e:
        LOGGER.exception(e)
        raise e

def get_ds_ver(
    session: db_session, project_id:int, user_id:int
    ) -> Dict[str, Any]:
    try:
        query_proj_user_pk = session.query(Project_user.pk).filter(and_(Project_user.project_id == project_id, \
            Project_user.user_id == user_id))
        query = session.query(Pdvu_mapping).filter(and_(Pdvu_mapping.project_user_id == query_proj_user_pk[0][0], \
            Pdvu_mapping.is_current == True))
        if query.count() != 0:
            pdvu_list = []
            for i in query:
                pdvu_list.append(i.to_dict())
            return {"status": 1, "msg": pdvu_list}
        else:
            return {"status": 0, "msg": "Entry not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)