import pytest

from db.db_proj_user import add_user_project, del_user_project, get_user_project
from db.db_projects import create_proj
from db.db_users import create_user


@pytest.mark.abc
def test_add_user_project(dbsession):
    create_user(dbsession, "asdf", "asdf", "asdf", "asdfasd", 1)
    proj_id = create_proj(dbsession, "test", "some desc", 1, 1)["msg"]
    res = add_user_project(dbsession, proj_id, 1, 2, True)
    assert res["msg"] == "User added"
    assert res["status"] == 1


@pytest.mark.abc
def test_get_user_project(dbsession):
    user_id = create_user(dbsession, "asdf", "asdf", "asdf", "asdfasd", 1)["msg"]
    proj_id = create_proj(dbsession, "test", "some desc", 1, 1)["msg"]
    add_user_project(dbsession, proj_id, 1, user_id, True)
    add_user_project(dbsession, proj_id, 1, 1, True)
    res = get_user_project(dbsession)
    assert len(res["msg"]) == 2
    assert res["msg"][0]["project_id"] == proj_id
    assert res["msg"][1]["project_id"] == proj_id
    assert res["msg"][0]["user_id"] == user_id
    assert res["msg"][1]["user_id"] == 1

    res = get_user_project(dbsession, 10)
    assert res["status"] == 0

    res = get_user_project(dbsession, proj_id)
    assert res["msg"][0]["project_id"] == proj_id
    assert res["msg"][1]["project_id"] == proj_id
    assert res["msg"][0]["user_id"] == user_id
    assert res["msg"][1]["user_id"] == 1

    res = get_user_project(dbsession, user_id=user_id)
    assert len(res["msg"]) == 1
    assert res["msg"][0]["user_id"] == user_id


@pytest.mark.abc
def test_del_user_project(dbsession):
    user_id = create_user(dbsession, "asdf", "asdf", "asdf", "asdfasd", 1)["msg"]
    proj_id = create_proj(dbsession, "test", "some desc", 1, 1)["msg"]
    add_user_project(dbsession, proj_id, 1, user_id, True)
    add_user_project(dbsession, proj_id, 1, 1, True)
    res = get_user_project(dbsession)
    assert len(res["msg"]) == 2

    res = del_user_project(dbsession, proj_id, user_id)

    assert res["status"] == 1

    res = get_user_project(dbsession)
    assert len(res["msg"]) == 1
