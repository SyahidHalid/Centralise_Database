# Database credentials
from pathlib import Path
import os
# UAT
CONNECTION_STRING = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.32.1.51,1455;'
    'DATABASE=mis_db_prod_30092025;'
    'UID=sa;'
    'PWD=Exim1234;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)
# Folder paths
# FOLDER_CONFIG = {
#     'FTP_directory': r'misPython_doc\\'
# }
# mis_db_prod18062025
# mis_db_prod_backup_2024_04_02

# # #PROD
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
# Folder paths
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

# FTP directory (use a valid key-value pair)
FOLDER_CONFIG = {
    "FTP_directory": os.path.join(PROJECT_ROOT,"misPython_doc") 
}

# # import config
# #  conn = pyodbc.connect(config.CONNECTION_STRING)

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# # TEST DB
# import os
# import sys
# import config
# import pyodbc
# import pandas as pd
# import numpy as np
# import datetime as dt

# # MIS PROD
# conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"+
#                 "Server=10.20.1.19,1455;"+
#                 "Database=mis_db_prod;"+
#                 "Trusted_Connection=no;"+
#                 "uid=mis_admin;"+
#                 "pwd=Exim1234")
# cursor = conn.cursor()
# LDB_prev = pd.read_sql_query("SELECT * FROM col_facilities_application_master;", conn)

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# import pyodbc
# import pandas as pd
# import os


# # CONNECT DB
# conn = pyodbc.connect(
#     "DRIVER={ODBC Driver 17 for SQL Server};"
#     "SERVER=10.20.1.25,1455;"
#     "UID=efms_admin;"
#     "PWD=Exim1234;"
# )
# cursor = conn.cursor()
# # MIS 10.20.1.19,1455 mis_admin Exim1234
# # EFMS 10.20.1.25,1455 efms_admin Exim1234
# # ECITS 10.20.1.27,1455 ecis_admin Exim1234
# # ECR 10.30.1.3,1455 ecr_admin Exim1234
# # UAT 10.32.1.51,1455 sa Exim1234
# # MIS NEW 10.20.1.4,1455

# # Query to list all databases and print
# cursor.execute("SELECT name FROM sys.databases;")

# print("Databases on this server:")
# for row in cursor.fetchall():
#     print("-", row[0])

# # enter DB
# cursor.execute("USE mis_db_prod;")
# cursor.execute("USE efacility_prod;")


# # Query tables (sys.tables, sys.views)
# sql = """
# SELECT s.name AS schema_name, t.name AS table_name
# FROM sys.views t
# JOIN sys.schemas s ON t.schema_id = s.schema_id
# ORDER BY s.name, t.name;
# """
# df = pd.read_sql_query(sql, conn)

# # Write to Excel
# output_path = "mis_db_prod_tables.xlsx"
# df.to_excel(output_path, sheet_name="tables", index=False, engine="openpyxl")
# print(f"Saved: {output_path}")


# print("Absolute path:", os.path.abspath(output_path))

# # MIS
# LDB_prev = pd.read_sql_query("SELECT * FROM col_facilities_application_master;", conn)

# #EFMS
# LDB_prev = pd.read_sql_query("SELECT * FROM view_budget;", conn)












