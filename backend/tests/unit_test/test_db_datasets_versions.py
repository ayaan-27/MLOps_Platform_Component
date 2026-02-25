import pytest

from db.db import (
    create_datasets_versions,
    delete_datasets_versions,
    read_datasets_versions,
    update_profiling_details,
)


@pytest.fixture(scope="session")
def mock_data_dataset_version():
    dataset_version = [
        {
            "is_deleted": False,
            "profiling_job_id": -1,
            "job_id": -1,
            "prev_id": -1,
            "user_id": 1,
            "version_id": 1,
            "profiling_done": False,
            "location": "Some location",
            "creation_time": 1626413558,
            "dataset_id": 1,
        },
        {
            "is_deleted": False,
            "profiling_job_id": -1,
            "job_id": -1,
            "prev_id": -1,
            "user_id": 1,
            "version_id": 2,
            "profiling_done": False,
            "location": "Some location",
            "creation_time": 1626413558,
            "dataset_id": 2,
        },
    ]
    yield dataset_version


# TODO
# job_id is set to default value as of now
@pytest.mark.a
def test_create_dataset_versions(dbsession):
    res = create_datasets_versions(dbsession, 1, 1, 1, "some_location")
    assert res["status"] == 1
    assert res["msg"] == 3


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, test_input3, expected1, expected2",
    [(1, True, 1, 1, 1), (1001, True, 1, 0, "dataset version not found")],
)
def test_update_profiling_details(
    dbsession, test_input1, test_input2, test_input3, expected1, expected2
):
    res = update_profiling_details(dbsession, test_input1, test_input2, test_input3)
    assert res["status"] == expected1
    assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (None, None, 1, 1),
        (1001, 1, 0, "dataset version not found"),
        (1, 1001, 0, "dataset version not found"),
    ],
)
def test_read_datasets_versions(
    dbsession, test_input1, test_input2, expected1, expected2, mock_data_dataset_version
):
    res = read_datasets_versions(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    if res["status"] == 1:
        assert all(elem in mock_data_dataset_version for elem in res["msg"])
    else:
        assert res["msg"] == expected2


@pytest.mark.a
@pytest.mark.parametrize(
    "test_input1, test_input2, expected1, expected2",
    [
        (1, 1, 1, 1),
        (1001, 1, 0, "dataset version not found"),
        (1, 1001, 0, "dataset version not found"),
    ],
)
def test_delete_datasets_versions(
    dbsession, test_input1, test_input2, expected1, expected2
):
    res = delete_datasets_versions(dbsession, test_input1, test_input2)
    assert res["status"] == expected1
    assert res["msg"] == expected2
