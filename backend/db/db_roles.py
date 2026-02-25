import time

from sqlalchemy import Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql

from db.db_metadata import Auto_repr, Base
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()
# role_id = -1 for owner


class Roles(Base, Auto_repr):
    __tablename__ = "roles"
    role_id = Column(Integer, primary_key=True)
    role_name = Column(postgresql.VARCHAR(50), nullable=False, unique=True)
    is_persona = Column(postgresql.BOOLEAN, nullable=False)
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    is_hidden = Column(postgresql.BOOLEAN, default=False)
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))


def read_roles(sess: db_session, role_id=None) -> Dict[str, Any]:
    """Reads roles from the db and returns it. Single Role's details can be read by providing the role ID.

    Args:
        sess (db_session): Database sess to use to get data.
        role_id (int, optional): Role ID, incase single role needs to be read. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary with status 1 if role details are read successfully.
    """
    try:
        query = sess.query(Roles)
        query = query.filter(and_(Roles.is_deleted == False, Roles.role_id != -1))

        if role_id is not None:
            query = query.filter(Roles.role_id.in_(role_id))
        roles_list = []
        if query.count() != 0:
            for role in query:
                roles_list.append(role.to_dict())

            return {"status": 1, "msg": roles_list}
        else:
            return {"status": 0, "msg": "Role(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def delete_role(sess: db_session, role_ids: list, commit=True) -> Dict[str, Any]:
    """Sets is_deleted to True in db for a given single role ID.

    Args:
        sess (db_session): Database sess to use to get data.
        role_id (int): Role ID.

    Returns:
        Dict[str, Any]: A dictionary with status 1 if a single role's is_deleted value is set to True
    """
    # delete roles for given role id
    try:
        query = sess.query(Roles).filter(Roles.role_id.in_(role_ids))
        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "Role ids deleted"}
        else:
            return {"status": 0, "msg": "Role not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)
