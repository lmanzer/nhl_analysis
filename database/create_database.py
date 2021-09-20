# Create Database
import sqlite3
from sqlite3 import Error
import logging

from settings import DATABASE_NAME

logging.basicConfig(level='INFO')
logger = logging.getLogger()


def create_database(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        logger.info(f'Database creation complete. Successfully created {db_file}')
    except Error as e:
        logger.error('Error: Database not created.')
        logger.error(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_database(DATABASE_NAME)
