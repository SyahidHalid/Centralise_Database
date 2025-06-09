import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.20.1.19,1455;'
    'DATABASE=mis_db_prod;'
    'UID=mis_admin;'
    'PWD=Exim1234;')

cursor = conn.cursor()

dbase_hist = pd.read_sql_query("SELECT * FROM col_facilities_application_master", conn)

#dbase_hist = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = '2025-04-30'", conn)

Test = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - FAD\\00. Loan Database\\Data Source\\202504\\Working\\Streamlit_04. Loan Database as at Apr 2025_Final v1.xlsx", sheet_name = "Loan Database", header=1)

dbase_hist[['facility_exim_account_num',
'acc_status',
'facility_ccy_id'
acc_disbursement_status
ca_post_approval_stage
fund_type
incentive
program_lending
guarantee

acc_payment_frequency_interest
acc_payment_frequency_principal
acc_effective_cost_borrowings

]]

Test1 = Test[['facility_exim_account_num',
'acc_status']]