import pytest

from db.db_users import *


@pytest.fixture(scope="session")
def mock_data_users():
    users = [
        {
            "creation_user_id": None,
            "creation_time": 1625143,
            "name": "admin",
            "org": None,
            "login_id": "admin",
            "is_deleted": False,
            "pwd": "super_admin",
            "user_id": 1,
        }
    ]
    yield users


@pytest.mark.a
def test_create_user(dbsession):
    res = create_user(dbsession, "trial_user", "Trial", "mphasis", "exy@123", 1)
    assert res["status"] == 1
    assert res["msg"] is not None


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, expected1, expected2",
    [(None, 1, "blank"), (1, 1, "blank"), (1001, 0, "user(s) not found")],
)
def test_read_users(dbsession, mock_data_users, test_input1, expected1, expected2):
    res = read_users(dbsession, test_input1)
    assert res["status"] == expected1
    if res["status"]:
        assert all(elem in mock_data_users for elem in res["msg"])
    else:
        assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        ("admin", "super_admin", 1, "blank"),
        ("admin", "wrong password", 0, "password does not match"),
        ("wrong admin", "super_admin", 0, "user not found"),
    ],
)
def test_val_user(
    dbsession, mock_data_users, test_input1, test_input2, expected1, expected2
):
    res = val_user(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    if res["status"]:
        assert all(elem in mock_data_users for elem in [res["msg"]])
    else:
        assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, expected1, expected2",
    [
        (
            1,
            1,
            1,
        ),
        (1001, 0, "user not found"),
    ],
)
def test_delete_user(dbsession, test_input1, expected1, expected2):
    res = delete_user(dbsession, test_input1)
    assert res["status"] == expected1
    assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, test_input3, expected1, expected2",
    [
        (1, "super_admin", "super_admin_two", 1, "password updated"),
        (1, "super_admin_error", "super_admin_two", 0, "current password does not match"),
        (1001, "super_admin", "super_admin_two", 0, "user not found"),
    ],
)
def test_update_pwd(
    dbsession, test_input1, test_input2, test_input3, expected1, expected2
):
    res = update_pwd(dbsession, test_input1, test_input2, test_input3)
    assert res["status"] == expected1
    assert res["msg"] == expected2
