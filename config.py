# Database credentials
CONNECTION_STRING = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.32.1.51,1455;'
    'DATABASE=mis_db_prod_backup_2024_04_02;'
    'UID=mis_admin;'
    'PWD=Exim1234;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)
# Folder paths
FOLDER_CONFIG = {
    'FTP_directory': r'D:\\mis_doc\\PythonProjects\\misPython\\misPython_doc\\'
}

# import config
#  conn = pyodbc.connect(config.CONNECTION_STRING)