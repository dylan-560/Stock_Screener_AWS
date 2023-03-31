"""
TO BE RUN ON FROM AWS LAMBDA
SAVES TO AWS TEST TABLES, PULLS SCREENS FROM AWS TEST TABLES
"""
# Test Case
RUN_AS_TEST = True

# Key Handling
PERSIST_KEY_INFO = True     # saves key info in between runs

SEND_SMS = True                                     # if true sends an sms to report any error types encountered
CREATE_REPORT = True
REPORT_ERROR_TYPES = ['ERROR', 'CRITICAL']          # error types to include in sms report
SAVE_ERRORS = True                                  # if true saves ERROR level logs to db

# Database
FINAL_SCREEN_RESULTS_TABLE_NAME = 'test_final_screen_results'
RAW_SCREEN_RESULTS_TABLE_NAME = 'test_daily_screen_results'

PULL_FROM_LOCAL = False
SAVE_TO_LOCAL = False

PULL_FROM_AWS = True
SAVE_TO_AWS = True

