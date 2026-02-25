import time

from sqlalchemy import Column, ForeignKey, Integer, PrimaryKeyConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.sqltypes import Boolean

from db.db_metadata import Auto_repr, Base
from db.db_permission import Permissions
from db.db_roles import Roles
from db.db_users import Users
from utils.custom_typing import Any, Dict, List, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Roles_permission_map(Base, Auto_repr):
    __tablename__ = "roles_permission_map"
    pk = Column(Integer, primary_key=True)
    role_id = Column(Integer, ForeignKey("roles.role_id"))
    perm_id = Column(Integer, ForeignKey("permissions.perm_id"))
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    # PrimaryKeyConstraint(role_id, perm_id)


def create_role(
    sess: db_session, role_name: str, creation_user_id: int, permissions: List[int]
) -> Dict[str, Any]:
    """Creates Role and maps to permission.

    Args:
        sess (db_session): Database sess
        role_name (str): Role name created
        creation_user_id (int): Created User ID
        permissions (List[int]): Permissions associated with the created role

    Raises:
        e: Raised Exception

    Returns:
        Dict[str, Any]: A dictionary with status 1 if role is created
    """
    # Creates Role and maps to permission
    try:
        role_ = Roles()
        role_.role_name = role_name
        role_.creation_user_id = creation_user_id
        sess.add(role_)
        sess.flush()
        role_id = role_.role_id
        perm_role_ = []
        for perm in permissions:
            role_permission_map_ = Roles_permission_map()
            role_permission_map_.role_id = role_id
            role_permission_map_.perm_id = perm
            role_permission_map_.creation_user_id = creation_user_id
            perm_role_.append(role_permission_map_)
        sess.add_all(perm_role_)
        sess.commit()
        return {"status": 1, "msg": role_id}

    except Exception as e:
        print(e)
        LOGGER.exception(e)
        sess.rollback()
        raise e


def create_role_persmission_map(
    sess: db_session, role_id: int, perm_id: int, creation_user_id: int, commit=True
) -> Dict[str, Any]:
    """Creates role to permission mapping.

    Args:
        sess (db_session): Database sess
        role_id (int): Single role ID for permission mapping
        perm_id (int): Single permission ID for role mapping
        creation_user_id (int): ##TO-DO

    Returns:
        Dict[str, Any]: A dictionary with status 1 if role ID is mapped with permission ID
    """
    # creates role to permission mapping
    try:
        role_permission_map_ = Roles_permission_map()
        role_permission_map_.role_id = role_id
        role_permission_map_.perm_id = perm_id
        role_permission_map_.creation_user_id = creation_user_id
        sess.add(role_permission_map_)
        if commit:
            sess.commit()
        else:
            sess.flush()
        return {"status": 1, "msg": role_id}
    except Exception as e:
        sess.rollback()
        LOGGER.exception(e)
        raise (e)


def read_role_permission_map(sess: db_session, role_id: int = None) -> Dict[str, Any]:
    """Returns list of all roles and permission

    Args:
        sess (db_session): Database sess
        role_id (int, optional): role ID for which roles and permissions are needed. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary with status 1 if roles are returned for the given role ID
    """
    # returns list of all roles and permission
    try:
        # subq = sess.query(Roles.role_id).filter(Roles.is_deleted == False)
        # query = sess.query(Roles_permission_map).filter(
        #     Roles_permission_map.role_id.in_(subq)
        # )
        query = sess.query(Roles_permission_map).filter(
            Roles_permission_map.is_deleted == False
        )
        if role_id is not None:
            query = query.filter(Roles_permission_map.role_id == role_id)

        roles_list = []
        if query.count() != 0:
            for role_ in query:
                roles_list.append(role_.to_dict())

            return {"status": 1, "msg": roles_list}
        else:
            return {"status": 0, "msg": "No Roles_permission_map found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def create_role(
    sess: db_session,
    role_name: str,
    creation_user_id: int,
    permissions: List[int],
    is_persona: Boolean,
    commit=True,
) -> Dict[str, Any]:
    # Creates Role and maps to permission
    try:
        role_ = Roles()
        role_.role_name = role_name
        role_.creation_user_id = creation_user_id
        role_.is_persona = is_persona
        sess.add(role_)
        sess.flush()
        role_id = role_.role_id
        perm_role_ = []
        for perm in permissions:
            role_permission_map_ = Roles_permission_map()
            role_permission_map_.role_id = role_id
            role_permission_map_.perm_id = perm
            role_permission_map_.creation_user_id = creation_user_id
            perm_role_.append(role_permission_map_)
        sess.add_all(perm_role_)
        if commit:
            sess.commit()
        else:
            sess.flush()
        return {"status": 1, "msg": role_id}

    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise e


def delete_role_permission_map(
    sess: db_session, role_id: list = None, perm_id: int = None, commit=True
) -> Dict[str, Any]:
    """Sets is_deleted to True for the given role ID.

    Args:
        sess (db_session): Database sess
        role_id (int): Single role ID that needs to be deleted

    Returns:
        Dict[str, Any]: A dictionary with status 1 if the role is deleted
    """
    try:
        query = sess.query(Roles_permission_map).filter(
            Roles_permission_map.is_deleted == False
        )

        if role_id is not None:
            query = query.filter(Roles_permission_map.role_id.in_(role_id))
        if perm_id is not None:
            query = query.filter(Roles_permission_map.perm_id == perm_id)

        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "Role Permission map deleted"}
        else:
            return {"status": 0, "msg": "Role not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)
