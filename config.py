# Database credentials

# UAT
CONNECTION_STRING = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.32.1.51,1455;'
    'DATABASE=mis_db_prod11072025;'
    'UID=sa;'
    'PWD=Exim1234;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)
# Folder paths
FOLDER_CONFIG = {
    'FTP_directory': r'misPython_doc\\'
}
# mis_db_prod18062025
# mis_db_prod_backup_2024_04_02

# #PROD
# CONNECTION_STRING = (
#     'DRIVER={ODBC Driver 17 for SQL Server};'
#     'SERVER=10.20.1.19,1455;'
#     'DATABASE=mis_db_prod;'
#     'UID=mis_admin;'
#     'PWD=Exim1234;'
#     'Encrypt=yes;TrustServerCertificate=yes;'
# )
# # Folder paths
# FOLDER_CONFIG = {
#     'FTP_directory': r'misPython_doc\\'
# }


# import config
#  conn = pyodbc.connect(config.CONNECTION_STRING)


# =================================UAT
# # Folder paths
# PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

# # FTP directory (use a valid key-value pair)
# FOLDER_CONFIG = {
#     "FTP_directory": os.path.join(PROJECT_ROOT,"misPython_doc") 
# }

# # import config
# #  conn = pyodbc.connect(config.CONNECTION_STRING)