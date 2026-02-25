import pytest

from db.db_roles import *


@pytest.fixture(scope="session")
def mock_data_roles():
    roles = [
        {
            "creation_time": 1625143,
            "creation_user_id": None,
            "role_name": "Admin",
            "role_id": 1,
            "is_deleted": False,
        }
    ]
    yield roles


@pytest.mark.b
def test_read_roles(dbsession, mock_data_roles):
    res = read_roles(dbsession)
    assert res["status"] == 1
    assert all(elem in mock_data_roles for elem in res["msg"])


@pytest.mark.b
def test_read_roles_id(dbsession, mock_data_roles):
    res = read_roles(dbsession, 1)
    assert res["status"] == 1
    assert all(elem in mock_data_roles for elem in res["msg"])


@pytest.mark.bc
def test_read_roles_id_error(dbsession):
    res = read_roles(dbsession, 1001)
    assert res["status"] == 0
    assert res["msg"] == "Role(s) not found"


@pytest.mark.a
def test_delete_role(dbsession):
    res = delete_role(dbsession, 1)
    assert res["status"] == 1
    assert res["msg"] == 1


@pytest.mark.a
def test_delete_role_error(dbsession):
    res = delete_role(dbsession, 5)
    assert res["status"] == 0
    assert res["msg"] == "Role not found"
