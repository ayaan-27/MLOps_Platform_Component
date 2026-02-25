import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.create_tables import create_table


@pytest.fixture(scope="session")
def engine():
    return create_engine("postgresql://pace_user:nextlabs123@localhost/pace")


@pytest.fixture(scope="session")
def tables(engine):
    create_table()
    yield
    create_table()


@pytest.fixture
def dbsession(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # roll back the broader transaction
    transaction.rollback()
    # put back the connection to the connection pool
    connection.close()
