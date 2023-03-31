"""
TO BE RUN ON LOCAL MACHINE
SAVES TO LOCAL TEST TABLES, PULLS SCREENS FROM LOCAL TEST TABLES
"""
# Test Case
RUN_AS_TEST = True

# Key Handling
PERSIST_KEY_INFO = True     # saves key info in between runs

# Error Reporting / Logging
SEND_SMS = False                                    # if true sends an sms to report any error types encountered
CREATE_REPORT = True
REPORT_ERROR_TYPES = ['ERROR', 'CRITICAL']          # error types to include in sms report
SAVE_ERRORS = True                                  # if true saves ERROR level logs to db

# Database
FINAL_SCREEN_RESULTS_TABLE_NAME = 'test_final_screen_results'
RAW_SCREEN_RESULTS_TABLE_NAME = 'test_daily_screen_results'

PULL_FROM_LOCAL = True
SAVE_TO_LOCAL = True

PULL_FROM_AWS = False
SAVE_TO_AWS = False