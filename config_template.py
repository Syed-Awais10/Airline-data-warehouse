# Configuration Template
# Copy this to config.py and add your actual credentials

# Database Configuration
SERVER = 'your_server_name'  # e.g., 'localhost'
DW_DB = 'AirlineDW'
CONN_STR = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DW_DB};Trusted_Connection=yes;"

# API Configuration
API_KEY = 'your_aviationstack_api_key_here'
API_URL = 'http://api.aviationstack.com/v1/flights'

# File Paths
CSV_PATH = 'path/to/your/test.csv'  # Update with your CSV location