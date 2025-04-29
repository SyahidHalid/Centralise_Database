import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

data = [
    ["330802137122029100", "I", "9/1/2025", "EXIM/LC/25/001", "WSA VENTURE AUSTRALIA (M) SDN BHD (FAC 3)", "PLANET ASIA PTE LTD", 205, "USD", 61200.00, "23/3/2025", "", 6],
    ["330801137110034000", "I", "17/1/2025", "EXIM/LC/25/003", "SITI KHADIJAH DAGANG SDN BHD (Fac: SITI KHADIJAH APPAREL SDN BHD)", "BSP (TAIWAN) CO., LTD.", 44, "USD", 43886.80, "10/5/2025", "", 6],
    ["330801137110034000", "I", "20/1/2025", "EXIM/LC/25/008", "SITI KHADIJAH DAGANG SDN BHD (Fac: SITI KHADIJAH APPAREL SDN BHD)", "PT KEWALRAM INDONESIA", 105, "USD", 80970.24, "25/2/2025", "", 6],
    ["330801137110034000", "R", "14/2/2025", "EXIM/LC/25/008", "SITI KHADIJAH DAGANG SDN BHD (Fac: SITI KHADIJAH APPAREL SDN BHD)", "PT KEWALRAM INDONESIA", 105, "USD", -80919.40, "25/2/2025", "", 6],
    ["330801137110034000", "I", "21/1/2025", "EXIM/LC/25/009", "SITI KHADIJAH DAGANG SDN BHD (Fac: SITI KHADIJAH APPAREL SDN BHD)", "HEBEI WOHUA TEXTILE CO., LTD", 44, "USD", 37113.04, "28/3/2025", "", 6],
    ["330802137122029100", "I", "12/2/2025", "EXIM/LC/25/010", "WSA VENTURE AUSTRALIA (M) SDN BHD (FAC 3)", "PLANET ASIA PTE LTD", 205, "USD", 93000.00, "30/4/2025", "", 6],
    ["330801137110038802", "I", "28/2/2025", "EXIM/LC/25/011", "FATHOPES ENERGY SDN BHD", "JARVIS INTERNATIONAL TRADE PTE LTD", 205, "USD", 242400.00, "31/3/2025", "", 6],
    ["330801137110034000", "I", "28/2/2025", "EXIM/LC/25/012", "SITI KHADIJAH DAGANG SDN BHD (Fac: SITI KHADIJAH APPAREL SDN BHD)", "PT KEWALRAM INDONESIA", 105, "USD", 14929.20, "10/4/2025", "", 6]
]

columns = [
    "accountNo", "typeBGLC", "drawdownDate", "tradeReferenceNo", "companyName",
    "beneficiaryOI", "countryOI", "currencyOI", "amountIssuedOI",
    "availabalityExpiryDate", "tradeRemark", "applicationStatus"
]

Test_jan1 = pd.DataFrame(data, columns=columns)

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.32.1.51,1455;'
    'DATABASE=mis_db_prod_backup_2024_04_02;'
    'UID=mis_admin;'
    'PWD=Exim1234;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)
#    'SERVER=10.32.1.51,1455;' UAT
#    'DATABASE=mis_db_prod_backup_2024_04_02;'

#    'SERVER=10.20.1.19,1455;' PROD
#    'DATABASE=mis_db_prod;'

cursor = conn.cursor()

#check ap yg exceed
#for col in Test_jan1.columns:
#    max_len = Test_jan1[col].astype(str).map(len).max()
#    print(f"{col}: max length = {max_len}")



#check ap yg exceed
# for index, row in Test_jan1.iterrows():
#     try:
#         cursor.execute(sql, tuple(row))
#     except Exception as e:
#         print(f"Error on row {index}: {e}")
#         print(row)
#         break

column_types = []
for col in Test_jan1.columns:
    dtype = Test_jan1[col].dtype
    if dtype == 'object':
        column_types.append(f"{col} VARCHAR(225)")
    elif dtype == 'int64':
        column_types.append(f"{col} INT")
    elif dtype == 'float64':
        column_types.append(f"{col} FLOAT")
    elif np.issubdtype(dtype, np.datetime64):
        column_types.append(f"{col} DATETIME")
    else:
        column_types.append(f"{col} VARCHAR(255)") 

# Generate the CREATE TABLE statement
create_table_query = "CREATE TABLE A_MYR (" + ', '.join(column_types) + ")"
# Execute the query
cursor.execute(create_table_query)

for row in Test_jan1.iterrows():
    sql = "INSERT INTO A_MYR({}) VALUES ({})".format(','.join(Test_jan1.columns), ','.join(['?']*len(Test_jan1.columns)))
    cursor.execute(sql, tuple(row[1]))
conn.commit()

cursor.execute("""INSERT INTO tradeMaster (	accountNo,
               typeBGLC,
               drawdownDate,
               tradeReferenceNo,
               companyName,
               beneficiaryOI,
               countryOI,
               currencyOI,
               amountIssuedOI,
               availabalityExpiryDate,
               tradeRemark,
               applicationStatus
)	
SELECT	
    accountNo,
               typeBGLC,
               drawdownDate,
               tradeReferenceNo,
               companyName,
               beneficiaryOI,
               countryOI,
               currencyOI,
               amountIssuedOI,
               availabalityExpiryDate,
               tradeRemark,
               applicationStatus
FROM A_MYR;	
""")

conn.commit() 

cursor.execute("drop table A_MYR")
conn.commit() 

conn.close()

# INSERT INTO table_name (column1, column2)
# SELECT column1, column2
# FROM other_table
# WHERE some_condition;