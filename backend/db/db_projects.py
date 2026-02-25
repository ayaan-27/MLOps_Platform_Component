import time

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import and_

from db.db_metadata import Auto_repr, Base
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Projects(Base, Auto_repr):
    __tablename__ = "projects"
    project_id = Column(Integer, primary_key=True)
    name = Column(postgresql.VARCHAR(100), nullable=False)
    proj_desc = Column(postgresql.VARCHAR(500), nullable=False)
    mlflow_id = Column(Integer, nullable=False)
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))


def create_proj(
    session: db_session,
    name: str,
    desc: str,
    user_id: int,
    mlflow_id: int,
    commit: bool = True,
) -> Dict[str, Any]:
    # table projects
    # create project
    try:
        proj_ = Projects()
        proj_.name = name
        proj_.proj_desc = desc
        proj_.mlflow_id = mlflow_id
        proj_.creation_user_id = user_id
        proj_.creation_time = int(time.time())
        session.add(proj_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": proj_.project_id}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def delete_proj(sess: db_session, project_id: int, commit: bool = True):
    try:
        query = sess.query(Projects).filter(
            and_(Projects.project_id == project_id, Projects.is_deleted == False)
        )
        if query.count() != 0:
            temp_ = query.first()
            temp_.is_deleted = True
            temp_.last_modified = int(time.time())
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "Project Deleted"}
        else:
            return {"status": 0, "msg": "Project Not found"}

    except Exception as e:
        LOGGER.exception(e)
        raise e


def edit_project_desc(sess: db_session, project_id: int, desc: str, commit: bool = True):
    try:
        query = sess.query(Projects).filter(
            and_(Projects.project_id == project_id, Projects.is_deleted == False)
        )
        if query.count() != 0:
            temp_ = query.first()
            temp_.proj_desc = desc
            temp_.last_modified = int(time.time())
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "Project Description Updated"}
        else:
            return {"status": 0, "msg": "Project Not found"}

    except Exception as e:
        LOGGER.exception(e)
        raise e
