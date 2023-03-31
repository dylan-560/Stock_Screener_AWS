from get_screens.settings._settings import *
from db_connect import Database

def send_SMS(msg):
    logging.info('-----------------------------------------------------------')
    logging.info('SENDING SMS')
    logging.info('-----------------------------------------------------------')
    try:
        from twilio.rest import Client

        # Set environment variables for your credentials
        # Read more at http://twil.io/secure
        account_sid = os.environ['TWILIO_ACCT_SID']
        auth_token = os.environ['TWILIO_AUTH_TOKEN']
        client = Client(account_sid, auth_token)

        twilio_balance = client.api.v2010.balance.fetch().balance

        msg += f'\nBalance Remaining: ${twilio_balance}'

        message = client.messages.create(
            body=msg,
            from_=os.environ['TWILIO_PHONE_NUMBER'],
            to=os.environ['MY_PHONE_NUMBER'])
    except Exception as e:
        logging.error(f'PROBLEM SENDING SMS, ERRORS: {e}')

def create_report_msg(func_name):
    """count number of errors and critical errors and sends sms """
    def get_runtime_errors():

        def pull_from_db():
            errors_list = []

            if PULL_FROM_LOCAL:
                db = Database(**LOCAL_DB_PARAMS)
                errors_list = db.pull_errors_data()

            elif PULL_FROM_AWS:
                db = Database(**AWS_DB_PARAMS)
                errors_list = db.pull_errors_data()

            return errors_list

        #####################################################

        errors_list = pull_from_db()

        errors_count_dict = dict([(e, 0) for e in REPORT_ERROR_TYPES])

        for error in errors_list:
            if error['level'] in REPORT_ERROR_TYPES:
                errors_count_dict[error['level']] += 1

        msg = ''
        for e,v in errors_count_dict.items():
            msg += f'\t{e}: {v}\n'

        return msg

    def get_key_errors():

        def pull_from_db():

            keys = {}

            if PULL_FROM_LOCAL:
                db = Database(**LOCAL_DB_PARAMS)
                keys = db.pull_key_info()

            elif PULL_FROM_AWS:
                db = Database(**AWS_DB_PARAMS)
                keys = db.pull_key_info()

            return keys

        #####################################################

        keys_dict = pull_from_db()

        keys_excluded_msg = ''
        for key_name, keys in keys_dict.items():
            if key_name == 'last_saved':
                continue

            for key in keys:
                if key['key_exclude']:
                    keys_excluded_msg += f'{key_name} excluded\n'

        return keys_excluded_msg

    ######################################################################
    if CREATE_REPORT:

        logging.info('-----------------------------------------------------------')
        logging.info('CREATING REPORT')
        logging.info('-----------------------------------------------------------')

        try:
            now = datetime_now()
            msg = f'\n{func_name}: {str(now.date())}\n'

            runtime_errors = get_runtime_errors()
            key_errors = get_key_errors()

            msg += f'RUNTIME ERRORS:\n{runtime_errors}'
            msg += f'KEYS MARKED EXCLUDED:\n{key_errors}'

            logging.info(msg)
            if SEND_SMS:
                send_SMS(msg=msg)

        except Exception as e:
            msg = f"PROBLEM GENERATING REPORT: {e}"
            logging.error(msg)
            record_errors(msg=msg)
