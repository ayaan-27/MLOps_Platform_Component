import time

from sqlalchemy import Boolean, Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_

from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()

class Deployment(Base, Auto_repr):
    __tablename__ = "deployment"
    d_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(postgresql.VARCHAR(100))
    model_id = Column(Integer, ForeignKey("model_version.model_id"))
    version_id = Column(Integer, ForeignKey("model_version.version_id"))
    user_id = Column(Integer, ForeignKey("users.user_id"))
    creation_time = Column(
        postgresql.BIGINT, nullable=False, default=int(time.time()))
    status = Column(postgresql.VARCHAR(50))
    last_modified = Column(postgresql.BIGINT)
    last_modified_by = Column(Integer, ForeignKey("users.user_id"))
    access_lvl = Column(postgresql.VARCHAR(50))
    end_point = Column(postgresql.VARCHAR(200))
    is_deleted = Column(postgresql.BOOLEAN, default=False)

def create_deployment(
    session: db_session,
    model_id: int,
    version_id: int,
    user_id: int,
    name:str,
    status:str,
    access_lvl:str,
    commit= True
) -> Dict[str, Any]:

    try:
        deployment_ = Deployment()
        deployment_.model_id = model_id
        deployment_.version_id = version_id
        deployment_.user_id = user_id
        deployment_.name = name
        deployment_.status = status
        deployment_.access_lvl = access_lvl
        deployment_.creation_time = int(time.time())    

        session.add(deployment_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": deployment_.d_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e

def delete_deployment(
    session: db_session,
    d_id: int,
    commit = True
) -> Dict[str, Any]:
    try:
        query = session.query(Deployment).filter(
            and_(
                Deployment.d_id == d_id, Deployment.is_deleted == False
                )
            )
        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "Deployment(s) Deleted"}
        else:
            return {"status": 0, "msg": "Deployment(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e

def read_deployments(
    session: db_session,
    user_id: int = None,
    d_id: int = None
) -> Dict[str, Any]:
    try:
        query = session.query(Deployment).filter(Deployment.is_deleted == False)
        if user_id:
            query = query.filter(Deployment.user_id == user_id)
        if d_id:
            query = query.filter(Deployment.d_id == d_id)
        deployment_list = []
        if query.count() != 0:
            for deployment in query:
                deployment_list.append(deployment.to_dict())
            return {"status": 1, "msg": deployment_list}
        else:
            return {"status": 0, "msg": "Deployment not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e
