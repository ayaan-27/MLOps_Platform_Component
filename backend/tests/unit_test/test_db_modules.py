import pytest

from db.db import get_module_name, read_modules


@pytest.fixture(scope="session")
def mock_data_modules():
    modules = [
        {"module_id": 1, "module_name": "Data Preprocessing"},
        {"module_id": 2, "module_name": "Feature Engneering"},
        {"module_id": 3, "module_name": "Data Augumentation"},
        {"module_id": 4, "module_name": "Projects"},
        {"module_id": 5, "module_name": "Role Management"},
        {"module_id": 6, "module_name": "User Management"},
        {"module_id": 7, "module_name": "Datasets"},
    ]
    yield modules


@pytest.mark.a
def test_read_modules(dbsession, mock_data_modules):
    res = read_modules(dbsession)
    assert res["status"] == 1
    assert all(elem in mock_data_modules for elem in res["msg"])


@pytest.mark.a
def test_read_module(dbsession, mock_data_modules):
    res = read_modules(dbsession, 2)
    assert res["status"] == 1
    assert all(elem in mock_data_modules for elem in res["msg"])


@pytest.mark.a
def test_read_modules_error(dbsession, mock_data_modules):
    res = read_modules(dbsession, 10)
    assert res["status"] == 0
    assert res["msg"] == "No Modules found"


@pytest.mark.a
def test_gete_module_name(dbsession, mock_data_modules):
    res = get_module_name(dbsession, 2)
    assert res["status"] == 1
    assert res["msg"] == "Feature Engneering"
