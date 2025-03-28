
import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

Test_jan1 = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\02. MIS Validation\\Listing Case Feb2025.xlsx", sheet_name = "Update SAP")

Test_jan1["facility_exim_account_num"] = Test_jan1["facility_exim_account_num"].astype(str)
Test_jan1.fillna("",inplace=True)

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.20.1.19,1455;'
    'DATABASE=mis_db_prod;'
    'UID=mis_admin;'
    'PWD=Exim1234;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)
#    'SERVER=10.32.1.51,1455;' UAT
#    'DATABASE=mis_db_prod_backup_2024_04_02;'

#    'SERVER=10.20.1.19,1455;' PROD
#    'DATABASE=mis_db_prod;'

cursor = conn.cursor()

column_types = []
for col in Test_jan1.columns:
    # You can choose to map column types based on data types in the DataFrame, for example:
    if Test_jan1[col].dtype == 'object':  # String data type
        column_types.append(f"{col} VARCHAR(255)")
    elif Test_jan1[col].dtype == 'int64':  # Integer data type
        column_types.append(f"{col} INT")
    elif Test_jan1[col].dtype == 'float64':  # Float data type
        column_types.append(f"{col} FLOAT")
    else:
        column_types.append(f"{col} VARCHAR(255)")  # Default type for others

# Generate the CREATE TABLE statement
create_table_query = "CREATE TABLE A_MYR (" + ', '.join(column_types) + ")"
# Execute the query
cursor.execute(create_table_query)

for row in Test_jan1.iterrows():
    sql = "INSERT INTO A_MYR({}) VALUES ({})".format(','.join(Test_jan1.columns), ','.join(['?']*len(Test_jan1.columns)))
    cursor.execute(sql, tuple(row[1]))
conn.commit()

cursor.execute("""MERGE INTO col_facilities_application_master AS target 
USING A_MYR AS source
ON target.facility_exim_account_num = source.facility_exim_account_num
WHEN MATCHED THEN
    UPDATE SET target.finance_sap_number = source.finance_sap_number;
""")
conn.commit() 

cursor.execute("drop table A_MYR")
conn.commit() 

conn.close()