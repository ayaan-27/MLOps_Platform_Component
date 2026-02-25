import os

import psycopg2

from confs.config import dbConfig
from utils.logs import get_logger

LOGGER = get_logger()


def connect_db(section="postgresql-aws"):
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # read connection parameters
        params = dbConfig(section=section)

        # connect to the PostgreSQL server
        LOGGER.info("Connecting to the PostgreSQL database")
        conn = psycopg2.connect(**params)
        LOGGER.info("Connected to PostgreSQL database")
        return conn
        # execute a statement
    except Exception as error:
        LOGGER.exception(error)
        raise error


def close_db(conn):
    try:
        if conn is not None:
            conn.close()
            LOGGER.info("Database connection closed")
    except Exception as error:
        LOGGER.exception(error)
        raise error


def create_table():
    try:
        conn = connect_db()
        cur = conn.cursor()
        LOGGER.info("Executing to the PostgreSQL database")
        cur.execute(
            open(os.path.join(os.path.dirname(__file__), "create_tables.sql"), "r").read()
        )
        conn.commit()
        cur.close()
        LOGGER.info("Tables created")
    except Exception as error:
        LOGGER.exception(error)
        raise error
    finally:
        close_db(conn)


if __name__ == "__main__":
    create_table()
