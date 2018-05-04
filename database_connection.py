#! usr/bin/env python3
import psycopg2
import json
import sys
import os
sys.path.append(os.environ["PROJECT_HOME"])
from config.database_connections import source_databases

base_queries = {"select_all": "select {} from {};",
                "select_random":" SELECT {} FROM {} ORDER BY random() LIMIT 1; ",
                "select_type":"select typname from pg_type where oid = {};",
                "select_null":"select * from {} where 1 = 2;",
                "select_tables":"select * from information_schema.tables;"}

class DBConnection(object):
    def __init__(self,source_name):
        datasource = source_databases.get(source_name)
        self.connection_string =  datasource.get("connection_details",None)
        self.connection_type = datasource.get("type",None)
        self.queries = base_queries
        self.open_connection()

    def open_connection(self):
        if self.connection_type == "postgres":
            self.conn = psycopg2.connect(self.connection_string)
        else:
            print("connection type not implemented")

    def close_connection(self):
        self.conn.close()

    def commit_connection(self):
        self.conn.commit()

    def get_cursor(self, name = None):
        return self.conn.cursor(name)

    def execute_cursor(self,stmt,commit = False):
        cursor = self.get_cursor("execute_stmt")
        cursor.execute(stmt)
        if commit:
            self.commit_connection()
        cursor.close()

    def execute_query(self,query):
        cursor = self.get_cursor("execute_query")
        cursor.execute(query)
        data = cursor.fetchall()
        description = cursor.description
        cursor.close()
        return data, description

    def stream_query(self,query):
#        print("stream query = {}".format(query))
        cursor = self.get_cursor("stream_query")
        cursor.execute(query)
        i = 0
        while True:
            result = cursor.fetchone()
            if result:
                i += 1
                yield result
            else:
                break

    def stream_table(self,table):
        base_query = self.queries.get("select_all")
        query = base_query.format("*",table)
        description = self.get_table_description(table)
#        print("streaming query {} ".format(query))
        generator = self.stream_query(query)
        return generator,description

    def get_base_tables(self):
        query = self.queries.get("select_tables")
        tables_list,description = self.execute_query()
        non_pg_tables = [table for table in tables_list if\
                         "pg" not in table[2] \
                         and table[1] != "information_schema"]
        return non_pg_tables

    def get_column_from_table(self,table,column) :
        base_query = self.queries.get("select_all")
        result = self.execute_query(base_query.format(column,table))[0]
        return  [x[0] for x in result]

    def watch_table(self,table):
        query = "select watch_table('{}','changes');"
        result = execute_query(query.format(table))
        return result

    def get_random_value_from_column(table,column):
        base_query = self.queries.get("select_random")
        result = self.execute_query(base_query.format(column,table))[0][0]
        return result

    def get_base_table_names():
        base_tables = self.get_base_tables()
        return [table[2] for table in base_tables]

    def get_type_name(oid):
        base_query = self.queries.get("select_type")
        names, description = self.execute_query(base_query.format(oid))
        return names[0][0]

    def get_base_table_descriptions():
        base_table_names = get_base_table_names()
        result = {}
        for table in base_table_names:
            description = self.get_table_description(table)
            description = {desc.name: get_type_name(desc.type_code) for desc in description}
            result.update({table:description})
        self.descriptions = result
        return result

    def get_table_description(self,table):
        base_query = self.queries.get("select_null")
        query_result,description = self.execute_query(base_query.format(table))
        return description

if __name__ == '__main__':
    db = source_databases.get("test_database")
    test = DBConnection(db)
    test.open_connection()
    generator,desc = test.stream_table("sales_orders")
    import time
    s = time.time()
    count = 0
    for i  in generator:
        pass
    e = time.time()
    speed = 7063000/(e - s)
    print(speed)
    with open("~/speed.txt","w+") as f:
        f.write("{} in {}s, {}r/s".format(703000,(e-s),speed))
