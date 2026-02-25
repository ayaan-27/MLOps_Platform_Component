from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.schema import UniqueConstraint

from db.db_metadata import Auto_repr, Base
from db.db_modules import Modules
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Permissions(Base, Auto_repr):
    __tablename__ = "permissions"
    perm_id = Column(Integer, primary_key=True)
    perm_lvl = Column(Integer, nullable=False, default=1)
    module_id = Column(Integer, ForeignKey("modules.module_id"))
    UniqueConstraint(perm_lvl, module_id)


def read_permissions(sess: db_session, perm_id: int = None) -> Dict[str, Any]:
    """Reads Permissions from the db and returns it. Single permission's details can be read by providing the permission ID.

    Args:
        session (db_session): DB session to use to get data.
        perm_id (int, optional): Permissioin ID, incase single permission needs to be read. Defaults to None.

    Returns:
        Dict[str, Any]: a dictionanry with Status 1 if Permission details are read sucessfully.
    """
    # returns permissions list
    try:
        query = sess.query(Permissions)

        if perm_id is not None:
            query = query.filter(Permissions.perm_id == perm_id)

        permissions_list = []
        if query.count() != 0:
            for perm in query:
                permissions_list.append(perm.to_dict())
            return {"status": 1, "msg": permissions_list}
        else:
            return {"status": 0, "msg": "No Permissions found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def get_perm_id(sess: db_session, perm_lvl: int, module_id: int):
    try:
        query = sess.query(Permissions.perm_id).filter(
            and_(Permissions.perm_lvl == perm_lvl, Permissions.module_id == module_id)
        )
        if query.count() != 0:
            return {"status": 1, "msg": query.scalar()}
        else:
            return {"status": 0, "msg": "No Permissions found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)
