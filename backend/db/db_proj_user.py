import time
from collections import Counter

from sqlalchemy import Column, ForeignKey, Integer, PrimaryKeyConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.functions import user

from db.db_metadata import Auto_repr, Base
from db.db_projects import Projects
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Project_user(Base, Auto_repr):
    __tablename__ = "project_user"
    pk = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.project_id"))
    user_id = Column(Integer, ForeignKey("users.user_id"))
    role_id = Column(Integer, ForeignKey("roles.role_id"))
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))


def add_user_project(
    sess: db_session,
    project_id: int,
    creation_user_id: int,
    user_id: int,
    role_id: int,
    commit: bool = True,
) -> Dict[str, Any]:
    # add users to project
    try:
        proj_user_ = Project_user()
        proj_user_.project_id = project_id
        proj_user_.user_id = user_id
        proj_user_.role_id = role_id
        proj_user_.creation_user_id = creation_user_id
        proj_user_.last_modified = int(time.time())
        sess.add(proj_user_)
        if commit:
            sess.commit()
        else:
            sess.flush()
        return {"status": 1, "msg": proj_user_.pk}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def delete_project_user(
    sess: db_session, project_id: int = None, user_ids: list = None, commit: bool = True
) -> Dict[str, Any]:
    try:
        query = sess.query(Project_user).filter(Project_user.is_deleted == False)
        if project_id is not None:
            query = query.filter(Project_user.project_id == project_id)
        if user_ids is not None:
            query = query.filter(Project_user.user_id.in_(user_ids))

        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "Project deleted from Project user table"}
        else:
            return {"status": 1, "msg": "No project user combo found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def change_user_role_project(
    sess: db_session,
    project_id: int,
    user_id: int,
    role_id: int,
    commit: bool = True,
) -> Dict[str, Any]:
    try:
        query = sess.query(Project_user).filter(
            and_(
                Project_user.project_id == project_id,
                Project_user.user_id == user_id,
                Project_user.is_deleted == False,
            )
        )
        if query.count() != 0:
            temp_ = query.first()
            temp_.role_id = role_id
            temp_.last_modified = int(time.time())
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "User role changed in project"}
        else:
            return {"status": 0, "msg": "Error in changing user role in project"}

    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def get_project_users(
    sess: db_session, proj_ids: list = None, user_id: int = None
) -> Dict[str, Any]:
    try:
        query = sess.query(Project_user).filter(Project_user.is_deleted == False)
        if proj_ids:
            query = query.filter(Project_user.project_id.in_(proj_ids))
        if user_id:
            query = query.filter(Project_user.user_id == user_id)
        if query.count() != 0:
            project_user_list = []
            for i in query:
                project_user_list.append(i.to_dict())
            return {"status": 1, "msg": project_user_list}
        else:
            return {"status": 0, "msg": "Project users not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def get_projects(
    sess: db_session, proj_id: int = None, user_id: int = None
) -> Dict[str, Any]:
    try:
        query = (
            sess.query(
                Projects.project_id,
                Projects.name,
                Projects.proj_desc,
                Projects.creation_user_id,
                Projects.creation_time,
                Project_user.user_id,
                Project_user.role_id,
            )
            .join(Projects, Project_user.project_id == Projects.project_id)
            .filter(Project_user.is_deleted == False)
        )
        if proj_id:
            query = query.filter(Project_user.project_id == proj_id)
        if user_id:
            query = query.filter(Project_user.user_id == user_id)
        if query.count() != 0:
            project_user_list = []
            for i in query:
                project_user = {}
                project_user["project_id"] = i[0]
                project_user["name"] = i[1]
                project_user["proj_desc"] = i[2]
                project_user["creation_user_id"] = i[3]
                project_user["creation_time"] = i[4]
                project_user["number_of_users"] = (
                    sess.query(Project_user.project_id)
                    .filter(
                        and_(
                            Project_user.project_id == i[0],
                            Project_user.is_deleted == False,
                        )
                    )
                    .count()
                )
                project_user_list.append(project_user)

            unique_project_list = list(
                {x["project_id"]: x for x in project_user_list}.values()
            )

            return {"status": 1, "msg": unique_project_list}
        else:
            return {"status": 0, "msg": "Project not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)
