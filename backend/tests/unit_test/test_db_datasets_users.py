import pytest

from db.db_datasets_users import *


@pytest.fixture(scope="session")
def mock_data_dataset_user():
    dataset_user = [
        {
            "creation_time": 1626413558,
            "user_id": 1,
            "dataset_id": 1,
            "is_deleted": False,
            "creation_user_id": None,
        },
        {
            "creation_time": 1626413558,
            "user_id": 1,
            "dataset_id": 2,
            "is_deleted": False,
            "creation_user_id": None,
        },
        {
            "creation_time": 1626413558,
            "user_id": 1,
            "dataset_id": 3,
            "is_deleted": False,
            "creation_user_id": None,
        },
    ]
    yield dataset_user


@pytest.mark.a
def test_create_datasets_user(dbsession):
    res = create_datasets_user(dbsession, 1, 1, 1)
    assert res["status"] == 1
    assert res["msg"] == 1


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (1, 1, 1, 1),
        (1, None, 1, 1),
        (None, 1, 1, 1),
        (None, None, 1, 1),
        (1001, 1, 0, "datasets User combination not found"),
        (1, 1001, 0, "datasets User combination not found"),
    ],
)
def test_read_datasets_user(
    dbsession, mock_data_dataset_user, test_input1, test_input2, expected1, expected2
):
    res = read_datasets_user(dbsession, test_input1, test_input2)
    res["status"] == expected1
    if res["status"]:
        assert all(elem in mock_data_dataset_user for elem in res["msg"])
    else:
        assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (1, 1, 1, 1),
        (1001, 1, 0, "datasets User combination not found"),
        (1, 1001, 0, "datasets User combination not found"),
    ],
)
def test_delete_datasets_versions(
    dbsession, test_input1, test_input2, expected1, expected2
):
    res = delete_dataset_user(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    assert res["msg"] == expected2
