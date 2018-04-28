#! usr/bin/env python3
import psycopg2
import json
from postgreslib.create_tables import get_conn_string

"""
functions in module :
	get_cursor
	commit_cursor
	close_cursor
	execute_query
    execute_cursor
"""

def get_cursor(return_cursor = False,return_connection = False):
    global CURSOR
    global CONNECTION
    conn_string = get_conn_string()
    CONNECTION = psycopg2.connect(conn_string)
    CURSOR = CONNECTION.cursor()
    result = []
    if return_cursor:
        result.append(CURSOR)
    if return_connection:
        result.append(CONNECTION)
    return result

def execute_cursor(stmt):
    global CURSOR
    CURSOR.execute(stmt)

def execute_query(query):
    global CURSOR
    execute_cursor(query)
    return CURSOR.fetchall(), CURSOR.description

def close_cursor():
    global CURSOR
    CURSOR.close()

def commit_connection():
    global CONNECTION
    CONNECTION.commit()

if __name__ == '__main__':
    pass
