import pytest

from db.db_datasets import *


@pytest.fixture(scope="session")
def mock_data():
    data = [
        {
            "creation_time": 1626413558,
            "user_id": 1,
            "dataset_id": 1,
            "name": "test_one",
            "is_deleted": False,
            "ds_desc": "test_two",
        },
        {
            "creation_time": 1626413558,
            "user_id": 1,
            "dataset_id": 2,
            "name": "dataset_two",
            "is_deleted": False,
            "ds_desc": "processed dataset",
        },
        {
            "creation_time": 1626413558,
            "user_id": 1,
            "dataset_id": 3,
            "name": "dataset_three",
            "is_deleted": False,
            "ds_desc": "final dataset",
        },
    ]
    yield data


@pytest.mark.mno
def test_create_dataset(dbsession):
    res = create_dataset(dbsession, "test", 1, "some description", True)
    assert res["status"] == 1
    assert res["msg"] == 4


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (None, 1, 1, None),
        (1, None, 1, None),
        (None, None, 1, None),
        (1001, 1001, 0, "dataset(s) not found"),
    ],
)
def test_read_datasets(
    dbsession, mock_data, test_input1, test_input2, expected1, expected2
):
    res = read_datasets(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    print(res["msg"])
    if res["status"] == 1:
        assert all(elem in mock_data for elem in res["msg"])
    else:
        assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (None, 1, 1, None),
        (1, None, 1, None),
        (None, None, 1, None),
        (1001, 1001, 0, "dataset(s) not found"),
    ],
)
def test_read_datasets(
    dbsession, mock_data, test_input1, test_input2, expected1, expected2
):
    res = read_datasets(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    if res["status"] == 1:
        assert all(elem in mock_data for elem in res["msg"])
    else:
        assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [(1, 1, 1, None), (1001, 1001, 0, "dataset not found")],
)
def test_delete_datasets(dbsession, test_input1, test_input2, expected1, expected2):
    res = delete_datasets(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    if res["status"] == 1:
        assert res["msg"] == 1
    else:
        assert res["msg"] == expected2
