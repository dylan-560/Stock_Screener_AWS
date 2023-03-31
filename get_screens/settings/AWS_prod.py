"""
FINAL PRODUCTION SETTINGS
TO BE RUN ON FROM AWS LAMBDA
SAVES TO AWS PROD TABLES, PULLS SCREENS FROM AWS PROD TABLES
"""
# Test Case
RUN_AS_TEST = False

# Key Handling
PERSIST_KEY_INFO = True     # saves key info in between runs

# Error Reporting / Logging
SEND_SMS = True                               # if true sends an sms to report any error types encountered
CREATE_REPORT = True
REPORT_ERROR_TYPES = ['ERROR', 'CRITICAL']      # error types to include in sms report
SAVE_ERRORS = True                              # if true saves ERROR level logs to db

# Database
FINAL_SCREEN_RESULTS_TABLE_NAME = '_final_screen_results'
RAW_SCREEN_RESULTS_TABLE_NAME = '_daily_screen_results'

PULL_FROM_LOCAL = False
SAVE_TO_LOCAL = False

PULL_FROM_AWS = True
SAVE_TO_AWS = True


