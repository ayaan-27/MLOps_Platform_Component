import pytest

from db.db_user_roles import *


@pytest.fixture(scope="session")
def mock_data_user_roles():
    user_roles = [
        {
            "creation_time": 1625143,
            "user_id": 1,
            "is_deleted": False,
            "creation_user_id": 1,
            "role_id": 1,
        }
    ]

    yield user_roles


@pytest.mark.a
def test_create_user_role(dbsession):
    res = create_user_role(dbsession, 1, 1, 1)
    assert res["status"] == 1
    assert res["msg"] == 1


@pytest.mark.a
def test_read_user_roles(dbsession, mock_data_user_roles):
    res = read_user_roles(dbsession)
    assert res["status"] == 1
    assert all(elem in mock_data_user_roles for elem in res["msg"])


@pytest.mark.a
def test_read_user_roles_id(dbsession, mock_data_user_roles):
    res = read_user_roles(dbsession, 1)
    assert res["status"] == 1
    assert all(elem in mock_data_user_roles for elem in res["msg"])


@pytest.mark.b
def test_read_user_roles_id_error(dbsession):
    res = read_user_roles(dbsession, 1001)
    print(res["msg"])
    assert res["status"] == 0
    assert res["msg"] == "user_roles not found"
