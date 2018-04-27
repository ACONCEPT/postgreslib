#! usr/bin/env python3
import psycopg2
import json
from create_tables import get_conn_string
"""
functions in module :
	get_cursor
	commit_cursor
	close_cursor
	execute_query
    execute_cursor
"""

def get_cursor():
    global CURSOR
    global CONNECTION
#    conn_string = "host='localhost' dbname='test' user='test' password='test'"
    conn_string = get_conn_string()
    CONNECTION = psycopg2.connect(conn_string)
    CURSOR = CONNECTION.cursor()
#    print(CURSOR)

def execute_cursor(stmt):
    global CURSOR
#    print("\n{}\n".format("executing {}".format(stmt)))
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
#    get_cursor()
#    print(execute_query("select * from parts;"))
