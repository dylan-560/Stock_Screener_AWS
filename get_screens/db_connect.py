import json
import time
import mysql.connector
from get_screens.settings._settings import *

TODAY = datetime_now()

try:
    import environment_vars
except:
    pass

class Database():

    def __init__(self,**params):
        self.conn = None
        self.cursor = None

        self.location = params['location']
        self.host_name = params['host']
        self.user = params['user']
        self.password = params['password']
        self.db_name = params['db_name']

    def create_connection(self):
        try:
            logging.info(f'\t\t\t ...creating connection to {self.location} db')

            self.conn = mysql.connector.connect(
                host=self.host_name,
                user=self.user,
                passwd=self.password,
                database=self.db_name,
                auth_plugin='mysql_native_password')

        except Exception as e:
            msg = f'{self.location} DB CONNECTION ERROR: {e}'
            logging.critical(f'{msg}')
            record_errors(msg=msg,level='CRITICAL')

    def close_connection(self):

        if self.conn and self.conn.is_connected():
            logging.info(f'\t\t\t ...closing connection to {self.location} db')
            if self.cursor:
                self.cursor.close()
            self.conn.close()
        else:
            logging.info(f'\t\t\t ...no {self.location} db connection to close')

    def establish_connection(self):
        if not self.conn or not self.conn.is_connected():
            self.create_connection()

        self.cursor = self.conn.cursor()

    def test_connection(self):
        logging.info(f'\t testing {self.location} db connection....')
        connection_attempts = 0
        connection_made = False

        while connection_attempts <= 5:
            self.create_connection()

            if not self.conn.is_connected():
                logging.info(f'\t\t problem connecting to {self.location} db, trying again....')
                time.sleep(5)
                connection_attempts += 1
            else:
                connection_made = True
                self.close_connection()
                return connection_made

        if not connection_made:
            msg = f'CANT ESTABLISH CONNECTION TO {self.location} DB'
            logging.critical(f'\t\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')
            return connection_made
        else:
            logging.info(f'\t\tCONNECTION TO {self.location} DATABASE ESTABLISHED')

    ######### RAW SCREEN RESULTS ################################################################
    def insert_daily_raw_screen_results(self, screens_list):
        logging.info(f'\t ...inserting raw screen data to {self.location} db')

        insert_successful = False

        try:
            self.establish_connection()

            for symbol_dict in screens_list:
                symbol_dict = json.dumps(symbol_dict)

                insert_date = str(TODAY.date())
                query = f"INSERT INTO {self.db_name}.{RAW_SCREEN_RESULTS_TABLE_NAME} (date, screen_result) VALUES (%s, %s);"

                values = (insert_date, symbol_dict)

                self.cursor.execute(query, values)

            self.conn.commit()
            insert_successful = True

        except Exception as e:
            msg = f'CANT INSERT RAW SCREEN DATA TO {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg,level='CRITICAL')

        finally:
            self.close_connection()

        logging.info(f'\t\t ...raw screens updated in {self.location} db')
        return insert_successful

    def pull_daily_raw_screen_results(self):
        logging.info(f'\t ...pulling raw screen data from {self.location} db')

        ret_list = []
        try:
            query = f'SELECT * FROM {self.db_name}.{RAW_SCREEN_RESULTS_TABLE_NAME} WHERE date = "{str(TODAY.date())}"'

            self.establish_connection()
            self.cursor.execute(query)
            screen_results = self.cursor.fetchall()

            ret_list = [json.loads(result[1]) for result in screen_results]

        except Exception as e:
            ret_list = []
            msg = f'CANT PULL RAW SCREEN DATA FROM {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')

        finally:
            self.close_connection()

        return ret_list

    def delete_daily_screen_results(self):
        logging.info(f'\t ...deleting raw screen data in {self.location} db '
                         f'older than {KEEP_RAW_SCREEN_DAYS} days')

        try:
            cutoff_date = str(TODAY.date() - datetime.timedelta(days=KEEP_RAW_SCREEN_DAYS))
            query = f'DELETE FROM {self.db_name}.{RAW_SCREEN_RESULTS_TABLE_NAME} WHERE date < "{cutoff_date}"'

            self.establish_connection()
            self.cursor.execute(query)
            self.conn.commit()

        except Exception as e:
            msg = f'CANT DELETE RAW SCREEN DATA IN {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')
        finally:
            self.close_connection()

        logging.info(f'\t\t ..raw screens deleted in {self.location} db')

    ######### FINAL SCREEN RESULTS ##############################################################

    def insert_final_results(self, screens_list):
        logging.info(f'\t ...updating final screen results data in {self.location} db')

        try:
            self.establish_connection()

            for screen in screens_list:
                insert_keys = []
                insert_values = []
                place_holders = []

                for k, v in screen.items():
                    insert_keys.append(k)
                    insert_values.append(v)
                    place_holders.append('%s')

                query = f"INSERT INTO {self.db_name}.{FINAL_SCREEN_RESULTS_TABLE_NAME} (" + ", ".join(insert_keys) + ")" \
                            " VALUES (" + ", ".join(place_holders) + ")"

                self.cursor.execute(query, insert_values)

            self.conn.commit()

            logging.info(f'\t\t ...updated final screen results in {self.location} db')

        except Exception as e:
            msg = f'CANT UPDATE FINAL SCREEN RESULTS DATA IN {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')
        finally:
            self.close_connection()

    def pull_final_results(self):
        logging.info(f'\t ...pulling final screen results data from {self.location} db')

        ret_list = []
        try:
            query = f"SELECT * FROM {self.db_name}.{FINAL_SCREEN_RESULTS_TABLE_NAME}"

            self.establish_connection()
            self.cursor.execute(query)
            screen_results = list(self.cursor.fetchall())

            for result in screen_results:
                ret_list.append(list(result))

        except Exception as e:
            msg = f'CANT PULL FINAL SCREEN RESULTS DATA FROM {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')

        finally:
            self.close_connection()

        return ret_list

    ######### KEYS INFO #########################################################################

    def update_key_info(self, keys):

        logging.info(f'\t ...updating key data in {self.location} db')

        try:
            query = f"SELECT COUNT(*) FROM {self.db_name}.{PERSISTED_KEYS_TABLE_NAME}"
            self.establish_connection()
            self.cursor.execute(query)
            result = self.cursor.fetchone()

            save_time = datetime_now()
            if result[0] == 0: # nothing exists, insert key stats
                logging.info(f'\t\t ...no keys found, inserting')

                for key_name, key_info in keys.items():
                    key_info = json.dumps(key_info)
                    query = f"INSERT INTO {self.db_name}.{PERSISTED_KEYS_TABLE_NAME} " \
                            f"(key_name, last_saved, key_info) " \
                            f"VALUES (%s, %s, %s);"

                    values = (key_name, save_time, key_info)
                    self.cursor.execute(query, values)

                self.conn.commit()

            else:
                logging.info(f'\t\t ...updating keys')
                for key_name, key_info in keys.items():
                    key_info = json.dumps(key_info)
                    query = f"UPDATE {self.db_name}.{PERSISTED_KEYS_TABLE_NAME} " \
                            f"SET key_info = %s, last_saved = %s" \
                            f"WHERE key_name = %s;"

                    values = (key_info, save_time, key_name)
                    self.cursor.execute(query,values)
                self.conn.commit()

        except Exception as e:
            msg = f'CANT UPDATE KEY DATA FROM {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')


        finally:
            self.close_connection()

    def pull_key_info(self):

        logging.info(f'\t ...pulling key data in {self.location} db')
        keys_dict = {}
        try:
            query = f"SELECT * FROM {self.db_name}.{PERSISTED_KEYS_TABLE_NAME}"

            self.establish_connection()
            self.cursor.execute(query)
            keys_list = self.cursor.fetchall()

            for keys in keys_list:

                key_name = keys[0]
                last_saved = keys[1]
                key_info = keys[2]

                if isinstance(key_info,str):
                    key_info = json.loads(key_info)

                keys_dict[key_name] = key_info
                keys_dict['last_saved'] = last_saved

        except Exception as e:
            msg = f'CANT PULL KEY INFO FROM {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')

        finally:
            self.close_connection()

        return keys_dict

    def delete_keys(self):
        logging.info(f'\t ...deleting key info in {self.location} db')

        try:
            query = f"DELETE FROM {self.db_name}.{PERSISTED_KEYS_TABLE_NAME}"
            self.establish_connection()
            self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            msg = f'CANT DELETE KEY INFO FROM {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')
        finally:
            self.close_connection()

    ######### ERRORS #############################################################################

    def insert_errors_data(self):
        logging.info(f'\t ...updating runtime errors in {self.location} db')

        if ERRORS_LIST:
            try:
                self.establish_connection()

                for error in ERRORS_LIST:
                    query = f"INSERT INTO {self.db_name}.{ERRORS_TABLE_NAME} " \
                            f"(datetime, error_level, message) " \
                            f"VALUES (%s, %s, %s);"

                    values = (
                        error['datetime'],
                        error['level'],
                        error['message']
                    )

                    self.cursor.execute(query, values)

                self.conn.commit()

                logging.info(f'\t\t ...updated runtime errors in {self.location} db')

            except Exception as e:
                msg = f'CANT UPDATE RUNTIME ERRORS DATA IN {self.location} DB: {e}'
                logging.critical(f'\t\t{msg}')
                record_errors(msg=msg, level='CRITICAL')
            finally:
                self.close_connection()

    def pull_errors_data(self):

        def convert_results_to_dict(results):
            for error in results:
                e_dict = {}
                for e in error:
                    if isinstance(e, datetime.datetime):
                        e_dict['datetime'] = e
                    elif e in REPORT_ERROR_TYPES:
                        e_dict['level'] = e
                    else:
                        e_dict['message'] = e

                ret_list.append(e_dict)

        ###############################################################################
        logging.info(f'\t ...pulling errors data from {self.location} db for {str(TODAY.date())}')

        ret_list = []
        try:

            cutoff_date = datetime.datetime.combine(TODAY, datetime.time.min)
            query = f'SELECT * FROM {self.db_name}.{ERRORS_TABLE_NAME} WHERE datetime >= "{str(cutoff_date)}"'

            self.establish_connection()
            self.cursor.execute(query)
            error_results = self.cursor.fetchall()
            convert_results_to_dict(results=error_results)

        except Exception as e:
            ret_list = []
            msg = f'CANT PULL ERRORS DATA FROM {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')

        finally:
            self.close_connection()

        return ret_list

    def delete_errors_data(self):
        logging.info(f'\t ...deleting old error data from {self.location} db')

        try:
            cutoff_date = str(TODAY - datetime.timedelta(days=KEEP_LOG_DAYS))
            query = f'DELETE FROM {self.db_name}.{ERRORS_TABLE_NAME} WHERE datetime <= "{str(cutoff_date)}"'
            self.establish_connection()
            self.cursor.execute(query)
            self.conn.commit()

        except Exception as e:
            msg = f'CANT DELETING ERROR DATA FROM {self.location} DB: {e}'
            logging.critical(f'\t\t{msg}')
            record_errors(msg=msg, level='CRITICAL')

        finally:
            self.close_connection()

