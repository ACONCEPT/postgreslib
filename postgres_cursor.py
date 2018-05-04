#! usr/bin/env python3
sys.path.append(os.environ["PROJECT_HOME"])
import psycopg2
import json
try:
    from postgreslib.create_tables import get_conn_string
except ImportError:
    from create_tables import get_conn_string
"""
functions in module :
	get_cursor
	commit_cursor
	close_cursor
	execute_query
    execute_cursor
"""
def contained_execute(func,*args,**kwargs):
    get_connection()
    get_cursor()
    func(*args,**kwargs)
    close_cursor()
    close_connection()

def get_connection(returnit = False):
    global CONNECTION
    conn_string = get_conn_string()
    CONNECTION = psycopg2.connect(conn_string)
    print("made connection {}".format(CONNECTION))

def get_cursor(return_cursor = False):
    global CURSOR
    global CONNECTION
    try:
        CURSOR = CONNECTION.cursor()
    except NameError as e:
        get_connection()
        CURSOR = CONNECTION.cursor()


def execute_cursor(stmt):
    global CURSOR
    global CONNECTION
    try:
        CURSOR.execute(stmt)
    except NameError as e :
        get_cursor()
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

def close_connection():
    global CONNECION
    CONNECTION.close()

def poll_for_notifications():
    global CONNECTION
    CONNECTION.poll()

if __name__ == '__main__':
    VAR
    pass
