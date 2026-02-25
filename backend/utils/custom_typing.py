from typing import Any, Dict, List, NewType

from sqlalchemy.orm import session

db_session = NewType("db_session", session.Session)
