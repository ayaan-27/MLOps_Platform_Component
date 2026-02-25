import time
import json

from sqlalchemy import Boolean, Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.expression import func
from sqlalchemy.sql.schema import PrimaryKeyConstraint

from db.db_model_hub import Model_hub
from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Model_version(Base, Auto_repr):
    __tablename__ = "model_version"
    model_id = Column(Integer, ForeignKey("model_hub.model_id"))
    model_version_name = Column(postgresql.VARCHAR(100))
    version_id = Column(Integer)
    mlflow_runid = Column(postgresql.VARCHAR(50), nullable=False)
    job_id = Column(Integer, ForeignKey("jobs_info.job_id"))
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    creation_time = Column(
        postgresql.BIGINT, nullable=False, default=int(time.time()))
    model_param = Column(postgresql.JSON, nullable=False)
    model_hyperparameters = Column(postgresql.JSON, nullable=False)
    pipeline_dict = Column(postgresql.JSON)
    PrimaryKeyConstraint(model_id, version_id)

def create_model_version(
    session: db_session,
    model_id:int,
    model_version_name:str,
    mlflow_runid: str,
    model_param:json,
    model_hyperparameters:json,
    job_id:int,
    commit: Boolean = True,
) -> Dict[str, Any]:
    try:
        model_version_ = Model_version()
        model_version_.model_id = model_id
        model_version_.mlflow_runid = mlflow_runid
        model_version_.job_id = job_id
        model_version_.creation_time = int(time.time())
        model_version_.model_param = model_param
        model_version_.model_version_name = model_version_name
        model_version_.model_hyperparameters = model_hyperparameters
        model_version_.version_id = get_max_model_ver_id(session, model_id)["msg"] + 1

        session.add(model_version_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": model_version_.version_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e

def udpate_pipeline_dict(
    session: db_session,
    model_id:int,
    version_id:int,
    pipeline_dict:json,
    commit: Boolean = True,
) -> Dict[str, Any]:
    try:
        query = session.query(Model_version).filter(and_(Model_version.model_id==model_id, Model_version.version_id==version_id))
        if query.count() !=0:
            query.update({"pipeline_dict": pipeline_dict})
            if commit:
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "Pipeline Dictionary updated"}
        else:
            return {"status": 0, "msg": "Unable to find model version"}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e


def get_max_model_ver_id(session: db_session, model_id: int) -> Dict[str, Any]:
    try:
        query = session.query(func.max(Model_version.version_id)).filter(
            Model_version.model_id == model_id
        )
        if query.scalar() is not None:
            max_ver_id = query.scalar()
            return {"status": 1, "msg": max_ver_id}
        else:
            # -1 so that + 1 makes it 0
            return {"status": 1, "msg": 0}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def read_models(
    session: db_session,
    user_id:int,
    model_id: int = None,
    version_id: int = None
) -> Dict[str, Any]:
    try:
        query = (
                session.query(
                        Model_hub.model_id,
                        Model_hub.model_name,
                        Model_hub.project_id,
                        Model_hub.user_id,
                        Model_version.job_id,
                        Model_version.model_version_name,
                        Model_version.version_id,
                        Model_version.mlflow_runid,
                        Model_version.creation_time,
                        Model_version.model_param,
                        Model_version.model_hyperparameters,
                        Model_version.pipeline_dict)
                        .join(Model_version, Model_hub.model_id == Model_version.model_id)
                        .filter(Model_hub.user_id==user_id,Model_version.is_deleted==False)
            )

        if model_id is not None:
            query = query.filter(Model_version.model_id == model_id)
        if version_id is not None:
            query = query.filter(Model_version.version_id == version_id)


        model_version_list = []
        temp = []
        if query.count() != 0:
            for ver in query:
                model_version = {}
                if ver[0] not in temp:
                    temp.append(ver[0])
                    model_version["model_id"] = ver[0]
                    model_version["model_name"] = ver[1]
                    model_version["project_id"] = ver[2]
                    model_version["user_id"] = ver[3]
                    model_version["job_id"] = ver[4]
                    model_version["model_version_name"] = ver[5]
                    model_version["version_id"] = ver[6]
                    model_version["mlflow_runid"] = ver[7]
                    model_version["creation_time"] = ver[8]
                    model_version["performance_metrics"] = ver[9]
                    model_version["model_hyperparameters"] = ver[10]
                    model_version["pipeline_dict"] = ver[11]
                    model_version_list.append(model_version)

            return {"status": 1, "msg": model_version_list}
        else:
            return {"status": 0, "msg": "model version not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e

def read_model_version(
    session: db_session,
    model_id: int = None,
    version_id: int = None
) -> Dict[str, Any]:
    try:
        query = (
                session.query(
                        Model_hub.model_id,
                        Model_hub.model_name,
                        Model_hub.project_id,
                        Model_hub.user_id,
                        Model_version.job_id,
                        Model_version.model_version_name,
                        Model_version.version_id,
                        Model_version.mlflow_runid,
                        Model_version.creation_time,
                        Model_version.model_param,
                        Model_version.model_hyperparameters,
                        Model_version.pipeline_dict)
                        .join(Model_version, Model_hub.model_id == Model_version.model_id)
                        .filter(Model_version.is_deleted==False)
            )

        if model_id is not None:
            query = query.filter(Model_version.model_id == model_id)
        if version_id is not None:
            query = query.filter(Model_version.version_id == version_id)


        model_version_list = []
        if query.count() != 0:
            for ver in query:
                model_version = {}
                model_version["model_id"] = ver[0]
                model_version["model_name"] = ver[1]
                model_version["project_id"] = ver[2]
                model_version["user_id"] = ver[3]
                model_version["job_id"] = ver[4]
                model_version["model_version_name"] = ver[5]
                model_version["version_id"] = ver[6]
                model_version["mlflow_runid"] = ver[7]
                model_version["creation_time"] = ver[8]
                model_version["performance_metrics"] = ver[9]
                model_version["model_hyperparameters"] = ver[10]
                model_version["pipeline_dict"] = ver[11]
                model_version_list.append(model_version)

            return {"status": 1, "msg": model_version_list}
        else:
            return {"status": 0, "msg": "model version not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e

def delete_model_versions(
    session: db_session, model_id: int, version_id: int = None, commit = True
) -> Dict[str, Any]:
    try:
        query = session.query(Model_version).filter(and_(
            Model_version.model_id == model_id, Model_version.is_deleted == False
            )
        )
        if version_id is not None:
            query = query.filter(Model_version.version_id == version_id)

        if query.count() != 0:
            query.update({"is_deleted": True})
            if commit:
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "Model version(s) deleted"}
        else:
            return {"status": 0, "msg": "Model version(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e

def get_job_id(session:db_session, model_id:int, version_id:int) -> Dict[str, Any]:
    try:
        query = session.query(Model_version.job_id).filter(
            Model_version.model_id == model_id, Model_version.version_id == version_id,
            Model_version.is_deleted == False
        )
        if query.count() != 0:
            job_id = query.scalar()
            return {"status": 1, "msg": job_id}
        else:
            return {"status": 0, "msg": "job_id not found"}        
    except Exception as e:
        LOGGER.exception(e)
        raise e   
