import psycopg2

# connect to your postgresql
connection=psycopg2.connect(
    host="localhost",
    database=" ",
    user=" ",
    password=" "
)

# your table name in the database
tablename=" "


FETCH_ALL_COLUMNS=f"""
SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name ='{tablename}';
"""
def get_columns():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(FETCH_ALL_COLUMNS)
            return cursor.fetchall()

lists=get_columns()

cleaned = [i[0] for i in lists]


def sql_column(clean):
    SELECT_COLUMNS = f"""
    SELECT * FROM {tablename} WHERE {clean} IS NULL;
    """
    return SELECT_COLUMNS

def sql_delete(clean):
    DELETE_COLUMNS = f"""
    ALTER TABLE {tablename} DROP {clean};
    """
    return DELETE_COLUMNS

def select_columns(clean):
    query1=sql_column(clean)
    print(query1)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query1)
            return  cursor.rowcount

def delete_columns(clean):
    query2 = sql_delete(clean)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query2)

#get the list of the column
lists=get_columns()

cleaned = ['"'+i[0]+'"'for i in lists]


for clean in cleaned:
    print(clean)
    # select_columns(clean)
    logic = select_columns(clean)
    print(logic)
    if logic != 0:
        delete_columns(clean)
        print("the column is deleted")
        # delete_columns()
    else:
        print("the column is saved")
    # print(clean)



