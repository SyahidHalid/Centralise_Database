
import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan


Test_jan = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\02. MIS Validation\\Test 20250313 _jan2025 (Public).xlsx", sheet_name = "MIS")

Test_jan = Test_jan.rename(columns={"Account Number":"Account_Number"})

Test_jan.Account_Number = Test_jan.Account_Number.str.replace("-","")

Test_jan1 = Test_jan[["Account_Number",
                      "LDB_P13_A",  #Cum Interest MYR                acc_accrued_interest_myr
                      "LDB_P13_B",  #Cum Principal Repayment MYR     acc_cumulative_repayment_myr
                      "LDB_P13_C",  #Cum Drawdown MYR                acc_cumulative_drawdown_myr
                      "LDB_P13_D"]].fillna(0) #Cum Other Payment MYR           acc_cumulative_others_charge_payment_myr

#Test_jan1.to_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\02. MIS Validation\\test.xlsx")
#Test_jan1["position_as_at"] = '2025-01-31'


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

#DF = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = '2025-01-31';", conn)
#facility_exim_account_num account without --

#invalid_data = Test_jan1[~Test_jan1.applymap(lambda x: isinstance(x, float) or pd.isna(x))]
#print(invalid_data)

# Assuming 'Test_jan1' is a DataFrame
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

cursor.execute("""MERGE INTO dbase_account_hist AS target 
USING A_MYR AS source
ON target.facility_exim_account_num = source.Account_Number
WHEN MATCHED AND target.position_as_at = '2025-01-31' THEN
    UPDATE SET target.acc_accrued_interest_myr = source.LDB_P13_A,
            target.acc_cumulative_repayment_myr = source.LDB_P13_B,
            target.acc_cumulative_drawdown_myr = source.LDB_P13_C,
            target.cumulative_other_charges_payment_myr = source.LDB_P13_D;
""")
conn.commit() 

cursor.execute("drop table A_MYR")
conn.commit() 

conn.close()

# SELECT [facility_exim_account_num]
#     ,[acc_accrued_interest_myr]
#     ,[acc_cumulative_repayment_myr]
#     ,[acc_cumulative_drawdown_myr]
#     ,[cumulative_other_charges_payment_myr]
# FROM [mis_db_prod].[dbo].[dbase_account_hist] where position_as_at = '2025-01-31' AND facility_exim_account_num = '330801137107031400'

# Select * from tradeMaster --where tradeMethod = 'BG'