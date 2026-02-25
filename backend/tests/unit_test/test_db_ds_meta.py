import pytest

from db.db_ds_meta import *


@pytest.fixture(scope="session")
def mock_meta_data():
    meta_data = [
        {
            "dataset_id": 1,
            "is_deleted": False,
            "col_info": "convert this to json",
            "row_count": 800,
            "version_id": 1,
            "ds_size": 23,
            "col_count": 80,
        }
    ]
    yield meta_data


@pytest.mark.adhd
def test_create_ds_meta(dbsession):
    res = create_ds_meta(dbsession, 2, 2, 200, 20, '{"info":"Some Info"}', 5)
    assert res["status"] == 1
    assert res["msg"] == 2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (1, 1, 1, 1),
        (1, None, 1, 1),
        (None, 1, 1, 1),
        (None, None, 1, 1),
        (1001, 1, 0, "No meta data found"),
        (1, 1001, 0, "No meta data found"),
    ],
)
def test_read_ds_meta(
    dbsession, mock_meta_data, test_input1, test_input2, expected1, expected2
):
    res = read_ds_meta(dbsession, test_input1, test_input2)
    res["status"] == expected1
    if res["status"]:
        assert all(elem in mock_meta_data for elem in res["msg"])
    else:
        assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (1, 1, 1, 1),
        (1001, 1, 0, "No meta data found"),
        (1, 1001, 0, "No meta data found"),
    ],
)
def test_delete_datasets_versions(
    dbsession, test_input1, test_input2, expected1, expected2
):
    res = delete_ds_meta(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    assert res["msg"] == expected2
