from get_screens.settings._settings import *
from db_connect import Database

def save_errors_to_DB():

    def save_to_db(db_params):
        db = Database(**db_params)
        db.insert_errors_data()

    ##################################################

    if ERRORS_LIST and SAVE_ERRORS:

        logging.info('-----------------------------------------------------------')
        logging.info('SAVING ERRORS TO DB')
        logging.info('-----------------------------------------------------------')

        if SAVE_TO_LOCAL:
            save_to_db(db_params=LOCAL_DB_PARAMS)
        if SAVE_TO_AWS:
            save_to_db(db_params=AWS_DB_PARAMS)

def delete_old_errors_from_DB():

    def delete_from_db(db_params):
        db = Database(**db_params)
        db.delete_errors_data()

    ##################################################

    logging.info('-----------------------------------------------------------')
    logging.info('DELETING ERRORS FROM DB')
    logging.info('-----------------------------------------------------------')

    if SAVE_TO_LOCAL:
        delete_from_db(db_params=LOCAL_DB_PARAMS)
    if SAVE_TO_AWS:
        delete_from_db(db_params=AWS_DB_PARAMS)
