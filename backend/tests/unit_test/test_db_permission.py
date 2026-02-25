import pytest

from db.db_permission import *


@pytest.fixture(scope="session")
def mock_data_permissions():
    perm = [
        {"module_id": 1, "perm_lvl": 1, "perm_id": 1},
        {"module_id": 1, "perm_lvl": 2, "perm_id": 2},
        {"module_id": 1, "perm_lvl": 3, "perm_id": 3},
        {"module_id": 1, "perm_lvl": 4, "perm_id": 4},
        {"module_id": 2, "perm_lvl": 1, "perm_id": 5},
        {"module_id": 2, "perm_lvl": 2, "perm_id": 6},
        {"module_id": 2, "perm_lvl": 3, "perm_id": 7},
        {"module_id": 2, "perm_lvl": 4, "perm_id": 8},
        {"module_id": 3, "perm_lvl": 1, "perm_id": 9},
        {"module_id": 3, "perm_lvl": 2, "perm_id": 10},
        {"module_id": 3, "perm_lvl": 3, "perm_id": 11},
        {"module_id": 3, "perm_lvl": 4, "perm_id": 12},
        {"module_id": 4, "perm_lvl": 1, "perm_id": 13},
        {"module_id": 4, "perm_lvl": 2, "perm_id": 14},
        {"module_id": 4, "perm_lvl": 3, "perm_id": 15},
        {"module_id": 4, "perm_lvl": 4, "perm_id": 16},
        {"module_id": 5, "perm_lvl": 1, "perm_id": 17},
        {"module_id": 5, "perm_lvl": 2, "perm_id": 18},
        {"module_id": 5, "perm_lvl": 3, "perm_id": 19},
        {"module_id": 5, "perm_lvl": 4, "perm_id": 20},
        {"module_id": 6, "perm_lvl": 1, "perm_id": 21},
        {"module_id": 6, "perm_lvl": 2, "perm_id": 22},
        {"module_id": 6, "perm_lvl": 3, "perm_id": 23},
        {"module_id": 6, "perm_lvl": 4, "perm_id": 24},
        {"module_id": 7, "perm_lvl": 1, "perm_id": 25},
        {"module_id": 7, "perm_lvl": 2, "perm_id": 26},
        {"module_id": 7, "perm_lvl": 3, "perm_id": 27},
        {"module_id": 7, "perm_lvl": 4, "perm_id": 28},
    ]
    yield perm


# @pytest.mark.skip(reason="testing other functions")
def test_read_permssions(dbsession, mock_data_permissions):
    res = read_permissions(dbsession)
    print(res["msg"])
    assert res["status"] == 1
    assert all(elem in mock_data_permissions for elem in res["msg"])


# @pytest.mark.skip(reason="testing other functions")
def test_read_permssion(dbsession, mock_data_permissions):
    res = read_permissions(dbsession, 3)
    assert res["status"] == 1
    assert all(elem in mock_data_permissions for elem in res["msg"])


# @pytest.mark.skip(reason="testing other functions")
def test_read_permssion_error(dbsession, mock_data_permissions):
    res = read_permissions(dbsession, 1001)
    assert res["status"] == 0
    assert res["msg"] == "No Permissions found"
