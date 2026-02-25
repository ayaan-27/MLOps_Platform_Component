"""from db.db_helper import read_modules
from db.db_helper import module_name
import pytest
from flask_sqlalchemy import SQLAlchemy
from db.table_map import Modules

@pytest.fixture
def module_data():
    data = {"status":0, "msg": [1, "Data Ingestion", 2, "Data Preprocessing"]}



def test_read_modules(db_session):
    res = db_session.query(Modules)
    assert res["status"] == 1 """
'''
from db.create_tables import create_table
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, session
from db.table_map import Base, Modules
from db.db_helper import read_modules, create_role
import pytest


@pytest.fixture(scope="module")
def engine():
    return create_engine("postgresql://pace_user:nextlabs123@localhost/pace")


@pytest.fixture(scope="module")
def tables(engine):
    create_table()
    yield
    create_table()


@pytest.fixture
def dbsession(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin_nested()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # roll back the broader transaction
    transaction.rollback()
    # put back the connection to the connection pool
    connection.close()

def test_read_modules(dbsession):
    res = read_modules()
    assert res["status"] == 1

def test_create_role(dbsession):
    res = create_role("trial", 1, [4,8])
    assert res["msg"] == 20
    res = create_role( "trial", 1, [4,8])
    assert res["msg"] == "Some error ocurred"

def create_role1(dbsession):
    res = create_role(dbsession, "trial", 1, [4,8])
    assert res["msg"] == 12

'''
