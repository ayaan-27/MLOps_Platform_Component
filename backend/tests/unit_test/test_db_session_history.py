import time

import pytest

from db.db import get_session_token


# noqc - check
@pytest.mark.abc
def test_read_modules(dbsession):
    res = get_session_token(dbsession, 1)
    assert res["status"] == 1
