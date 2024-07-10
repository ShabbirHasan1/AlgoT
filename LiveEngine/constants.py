#
##################
# CONSTANTS FILE #
##################
#
# Do Not Temper with the values of the constants
#
#
########################################################
# Program Logic Constants
FETCH_INTERVAL_SECONDS = 5
UPDATE_15MIN_DF_INTERVAL = 10
compression_value_15_min = 900
compression_value_1_min = 60
ORDER_PLACER_INTERVAL = 5
########################################################
#
#
########################################################
# Symphony Fintech API Constants
API_KEY = '3642a41c547ee103920202'
API_SECRET = 'Qkbc461#aF'
SOURCE = 'WebAPI'
ROOT_URL = 'http://ctrade.jainam.in:3000/apimarketdata'
########################################################
#
#
########################################################
# SQL Database Constants
SQL_USERNAME = 'hirakdesai'
SQL_PASSWORD = ''
SQL_HOST = 'localhost'
SQL_DATABASE = 'symphony'
SQL_PORT = 3306
########################################################
#
#
########################################################
# Spot Brain Constants
WINDOW = 14 # window for 15-minute candles
FAST_FACTOR = 2.0
SLOW_FACTOR = 30.0
DF_15MIN_BACK_DAYS = 6
########################################################
# Symphony Interactive API Constants
INTERACTIVE_API_KEY = '89e06b0bb84ea9b81c7588'
INTERACTIVE_API_SECRET = 'Wgcs076@Z0'
INTERACTIVE_SOURCE = 'WebAPI'
INTERACTIVE_ROOT_URL = 'http://ctrade.jainam.in:3000'
#
#
########################################################
# Telegram Bot Constants
TELEGRAM_BOT_TOKEN = "6418611543:AAFgPRBSBjiaSpcH8pJawpaWUrkjN_AOrto"
TRENDLINES_CHAT_ID = "-4222452040"
#
#
########################################################
# File Paths
DAILY_JSON_PATH = "LiveEngine/daily.json"