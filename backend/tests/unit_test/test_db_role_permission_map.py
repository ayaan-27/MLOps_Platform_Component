import pytest

from db.db_roles_permission_map import *


@pytest.fixture(scope="session")
def mock_data_roles_permission():
    perm = [
        {
            "role_id": 1,
            "creation_user_id": None,
            "creation_time": 1625143,
            "perm_id": 4,
            "is_deleted": False,
        },
        {
            "role_id": 1,
            "creation_user_id": None,
            "creation_time": 1625143,
            "perm_id": 8,
            "is_deleted": False,
        },
        {
            "role_id": 1,
            "creation_user_id": None,
            "creation_time": 1625143,
            "perm_id": 12,
            "is_deleted": False,
        },
        {
            "role_id": 1,
            "creation_user_id": None,
            "creation_time": 1625143,
            "perm_id": 16,
            "is_deleted": False,
        },
        {
            "role_id": 1,
            "creation_user_id": None,
            "creation_time": 1625143,
            "perm_id": 20,
            "is_deleted": False,
        },
        {
            "role_id": 1,
            "creation_user_id": None,
            "creation_time": 1625143,
            "perm_id": 24,
            "is_deleted": False,
        },
        {
            "role_id": 1,
            "creation_user_id": None,
            "creation_time": 1625143,
            "perm_id": 28,
            "is_deleted": False,
        },
    ]
    yield perm


@pytest.mark.a
def test_create_role(dbsession):
    res = create_role(dbsession, "trial", 1, [4, 8])
    assert res["status"] == 1
    assert res["msg"] == 2


@pytest.mark.a
def test_create_role_persmission_map(dbsession):
    res = create_role_persmission_map(dbsession, 1, 1, 1)
    assert res["status"] == 1
    assert res["msg"] == 1


@pytest.mark.a
def test_read_role_permission_map(dbsession, mock_data_roles_permission):
    res = read_role_permission_map(dbsession)
    assert res["status"] == 1
    print(res["msg"])
    assert all(elem in mock_data_roles_permission for elem in res["msg"])


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input, expected1, expected2",
    [(1, 1, "blank"), (1001, 0, "No Roles_permission_map found")],
)
def test_read_role_permission_map_id(
    dbsession, mock_data_roles_permission, test_input, expected1, expected2
):
    res = read_role_permission_map(dbsession, test_input)
    assert res["status"] == expected1
    if res["status"]:
        assert all(elem in mock_data_roles_permission for elem in res["msg"])
    else:
        assert res["msg"] == "No Roles_permission_map found"
