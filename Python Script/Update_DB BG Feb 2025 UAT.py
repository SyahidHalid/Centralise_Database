import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

#Test_jan = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - FAD\\00. Loan Database\\Data Source\\202502\\Trade\\BG and LC Cont. Liab. FEB 2025 (with account).xlsx", sheet_name = "BG - FEB 2025 ACTIVE ", header=1)
Test_jan = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - FAD\\00. Loan Database\\Data Source\\202502\\Trade\\BG and LC Cont. Liab. FEB 2025 (with account).xlsx", sheet_name = "Sheet1", header=0)

Test_jan1 = Test_jan[['accountNo',
                      'companyName',
                      'guarateeTypeOI',
                      'beneficiaryOI',
                      'counterIssuingBankOI',
                      'guaranteeTypeCounterIssuingBankOI',
                      'countryOI',
                      'tradeReferenceNo',
                      'otherFacilityOI',
                      'currencyOI',
                      'amountIssuedOI',
                      'drawdownDate',
                      'maturityDate',
                      'extendedExpiryDate',
                      'tradeRemark',
                      'amountApproved',
                      'facilityBankGuarantee',
                      'tradeMethod',
                      'serialNumber',
                      'applicationStatus']]#.fillna(0)

Test_jan1["accountNo"] = Test_jan1["accountNo"].astype(str)
Test_jan1["countryOI"] = Test_jan1["countryOI"].astype(str)
Test_jan1["serialNumber"] = Test_jan1["serialNumber"].astype(str)


Test_jan1["drawdownDate"] = Test_jan1["drawdownDate"].astype(str)
Test_jan1["maturityDate"] = Test_jan1["maturityDate"].astype(str)
Test_jan1["extendedExpiryDate"] = Test_jan1["extendedExpiryDate"].astype(str)

Test_jan1['tradeRemark'] = Test_jan1['tradeRemark'].str[-255:]

Test_jan1.tail(1)
Test_jan1.shape
Test_jan1.dtypes

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

cursor.execute("""INSERT INTO tradeMaster (	
    tradeReferenceNo,	
    drawdownDate,	
    maturityDate,	
    extendedExpiryDate,	
    facilityBankGuarantee,	
    guarateeTypeOI,	
    beneficiaryOI,	
    counterIssuingBankOI,	
    guaranteeTypeCounterIssuingBankOI,	
    otherFacilityOI,	
    countryOI,	
    currencyOI,	
    amountIssuedOI,	
    amountApproved,	
    tradeMethod,	
    companyName,	
    tradeRemark,	
	serialNumber,
	accountNo,
    applicationStatus
)	
SELECT	
    tradeReferenceNo,	
    drawdownDate,	
    maturityDate,	
    extendedExpiryDate,	
    facilityBankGuarantee,	
    guarateeTypeOI,	
    beneficiaryOI,	
    counterIssuingBankOI,	
    guaranteeTypeCounterIssuingBankOI,	
    otherFacilityOI,	
    countryOI,	
    currencyOI,	
    amountIssuedOI, 	
    amountApproved,	
    tradeMethod,	
    companyName,	
    tradeRemark,	
	serialNumber,
	accountNo,
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