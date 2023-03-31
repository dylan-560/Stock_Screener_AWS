# get size of both datasets
# loop through the largest one
# get ticker and datetime
#   look to match ticker and datetime to smaller db
#   if matches, next
#   if not fill in the row
# when done reset the primary keys of both dbs

import mysql.connector
from get_screens.settings._settings import *

def reconcile_tables():

    def create_DB_connection(**creds):
        conn = mysql.connector.connect(
            host=creds['host'],
            user=creds['user'],
            passwd=creds['password'],
            database=creds['db_name'],
            auth_plugin='mysql_native_password')

        return conn

    def get_table_count(conn):
        cursor = conn.cursor()

        query = f" SELECT COUNT(*) FROM {table_name}"
        cursor.execute(query)
        row_count = cursor.fetchall()
        row_count = row_count[0][0]

        cursor.close()

        return row_count

    def pull_identifiers_from_table():
        cursor = from_db.cursor(dictionary=True)
        query = f'SELECT symbol, datetime, prev_close, max_pct_gain FROM screener_data.{table_name}'
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()

        return data

    def get_required_update_rows():

        def row_exists():
            query = f'SELECT * FROM {table_name} ' \
                    f'WHERE symbol = "{identifiers["symbol"]}" AND ' \
                    f'datetime = "{str(identifiers["datetime"])}" AND ' \
                    f'prev_close = {identifiers["prev_close"]} AND '  \
                    f'max_pct_gain = {identifiers["max_pct_gain"]};'

            to_db_cursor.execute(query)
            data = to_db_cursor.fetchall()
            return bool(data)

        #########################################
        rows_to_update = []
        to_db_cursor = to_db.cursor()

        print('\t\t\t need to update rows...')
        for identifiers in from_data:
            if not row_exists():
                rows_to_update.append(identifiers)
                print(f'\t\t\t\t {identifiers["symbol"]}, {identifiers["datetime"]}')
        to_db_cursor.close()
        return rows_to_update

    def update_to_DB():

        def pull_full_row_from_DB():
            query = f'SELECT * FROM screener_data.{table_name} ' \
                    f'WHERE symbol = "{row["symbol"]}" AND ' \
                    f'datetime = "{str(row["datetime"])}" AND ' \
                    f'prev_close = {row["prev_close"]} AND ' \
                    f'max_pct_gain = {row["max_pct_gain"]};'

            from_db_cursor.execute(query)
            full_row_data = from_db_cursor.fetchall()

            return full_row_data

        def insert_row_to_DB():
            print(f'\t\t inserting {full_row["symbol"]} {full_row["datetime"]}')
            insert_keys = []
            insert_values = []
            place_holders = []

            for k, v in full_row[0].items():
                insert_keys.append(k)
                insert_values.append(v)
                place_holders.append('%s')

            query = f"INSERT INTO screener_data.{table_name} (" + ", ".join(insert_keys) + ") VALUES (" + ", ".join(place_holders) + ")"

            to_db_cursor.execute(query, insert_values)
            to_db.commit()

        ##############################################

        from_db_cursor = from_db.cursor(dictionary=True)
        to_db_cursor = to_db.cursor()

        for row in rows_to_update:
            full_row = pull_full_row_from_DB()
            insert_row_to_DB()


    ##################################################
    print('--------------------------------------------------------------')
    print('RECONCILING LOCAL AND AWS DATABASES')
    print('--------------------------------------------------------------')
    table_name = 'test_final_screen_results'

    local_conn = create_DB_connection(**LOCAL_DB_PARAMS)
    aws_conn = create_DB_connection(**AWS_DB_PARAMS)

    local_row_count = get_table_count(conn=local_conn)
    aws_row_count = get_table_count(conn=aws_conn)

    if local_row_count != aws_row_count:
        print('\tTABLES ARE ASYMMETRICAL')

        if local_row_count > aws_row_count:
            from_db = local_conn
            to_db = aws_conn
            print('\t\t updating AWS DB from local DB')
        elif aws_row_count > local_row_count:
            from_db = aws_conn
            to_db = local_conn
            print('\t\t updating local DB from AWS DB')

        from_data = pull_identifiers_from_table()
        rows_to_update = get_required_update_rows()
        update_to_DB()





reconcile_tables()



