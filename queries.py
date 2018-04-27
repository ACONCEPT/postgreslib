#! usr/bin/env python3
from postgres_cursor import get_cursor, execute_query, commit_connection, close_cursor, execute_cursor
import json

def main():
	pass

def get_base_table_descriptions():
    base_table_names = get_base_table_names()
    base_query = "select * from {} where 1 = 2;"
    result = {}
    for table in base_table_names:
        query_result,description = execute_query(base_query.format(table))
        description = {desc.name: get_type_name(desc.type_code) for desc in description}
        result.update({table:description})

    with open("base_table_definitions.json","w+") as f:
        f.write(json.dumps(result))
    return result

def get_type_name(oid):
    base_query = "select typname from pg_type where oid = {};"
    names, description = execute_query(base_query.format(oid))
    return names[0][0]

def get_base_table_names():
    base_tables = get_base_tables()
    return [table[2] for table in base_tables]

def get_random_value_from_column(table,column):
    base_query = """
SELECT {}
FROM {}
ORDER BY random()
LIMIT 1;
"""
    result = execute_query(base_query.format(column,table))[0][0]
#    print(result)
    return result

def get_column_from_table(table,column) :
    base_query = """
select {}
from {};
"""
    result = execute_query(base_query.format(column,table))[0]
    return  [x[0] for x in result]

def get_base_tables():
    get_cursor()
    tables_list,description = execute_query("select * from information_schema.tables;")
    non_pg_tables = [table for table in tables_list if\
                     "pg" not in table[2] \
                     and table[1] != "information_schema"]
    return non_pg_tables

if __name__ == '__main__':
    get_cursor()
#    customer_list = get_column_from_table("customers","id")
#    print(customer_list)
    close_cursor()

#    with open("base_table_definitions.json","r") as f:
#        definitions = json.loads(f.read())
#    for key,item in definitions.items():
#        print(key,item)
