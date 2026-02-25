import pytest

from db.db_projects import *


@pytest.mark.abc
@pytest.mark.parametrize(
    "name,desc,user_id,mlflow_id,output_msg", [("test", "erasedf asfew r", 1, 1, 1)]
)
def test_create_proj(dbsession, name, desc, user_id, mlflow_id, output_msg):
    res = create_proj(dbsession, name, desc, user_id, mlflow_id)
    assert res["status"] == output_msg
    assert res["msg"] is not None


@pytest.mark.abc
def test_get_proj(dbsession):
    res = create_proj(dbsession, "test", "some description", 1, 1)
    res = create_proj(dbsession, "test2", "some description", 1, 2)
    proj_id = res["msg"]
    res = get_projects(dbsession)
    assert res["status"] == 1
    assert len(res["msg"]) == 2
    assert res["msg"][0]["name"] == "test"
    assert res["msg"][0]["mlflow_id"] == 1
    assert res["msg"][1]["mlflow_id"] == 2
    assert res["msg"][1]["name"] == "test2"

    res = get_projects(dbsession, proj_id)
    assert res["status"] == 1
    assert len(res["msg"]) == 1
    assert res["msg"][0]["mlflow_id"] == 2
    assert res["msg"][0]["name"] == "test2"


@pytest.mark.abc
def test_delete_proj(dbsession):
    res = create_proj(dbsession, "test", "some description", 1, 1)
    proj_id = res["msg"]
    res = create_proj(dbsession, "test2", "some description", 1, 2)
    res = delete_proj(dbsession, proj_id)
    assert res["status"] == 1
    res = get_projects(dbsession)
    assert len(res["msg"]) == 1
