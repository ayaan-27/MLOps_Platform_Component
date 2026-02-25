from sqlalchemy import Column, Integer
from sqlalchemy.dialects import postgresql

from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Modules(Base, Auto_repr):
    __tablename__ = "modules"
    module_id = Column(Integer, primary_key=True)
    module_name = Column(postgresql.VARCHAR(100), nullable=False)


def read_modules(session: db_session, module_id: int = None) -> Dict[str, Any]:
    """Reads Modules from the db and returns it. Single module's details can be read by providing the module ID.

    Args:
        session (db_session): Database session to use to get data.
        module_id (int, optional): Module ID, incase single module needs to be read. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary with status 1 if module detailes are read successfully.
    """
    try:
        query = session.query(Modules)
        if module_id is not None:
            query = query.filter(Modules.module_id == module_id)

        module_list = []
        if query.count() != 0:
            for role in query:
                module_list.append(role.to_dict())
            return {"status": 1, "msg": module_list}
        else:
            return {"status": 0, "msg": "No Modules found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def get_module_name(session: db_session, module_id: int) -> Dict[str, Any]:
    """Reads module name from the db and returns it. Single module's name can be read by providing the module ID.

    Args:
        session (db_session): Database session to use to get data.
        module_id (int): Module ID, incase single module's name to be read.

    Returns:
        Dict[str, Any]: A dictionary with status 1 if module name is read is successfully.
    """
    try:
        query = session.query(Modules).filter(Modules.module_id == module_id)
        if query.count() != 0:
            return {"status": 1, "msg": query.first().to_dict()["module_name"]}
        else:
            return {"status": 0, "msg": "No Modules found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)
