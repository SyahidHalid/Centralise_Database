# python Debtor_Listing.py 11,"Debtors Listing and Customer Balance Report as at October 2024_Adjusted.xlsx","Debtor Listing","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python Debtor_Listing.py 11 "Debtors Listing and Customer Balance Report as at October 2024_Adjusted.xlsx" "Debtor Listing" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-03-29"
# position_as_at
#aftd_id = DocumentId
#tmbh update result table

#try:
import config
import os
import sys
import pyodbc
#from config import PROJECT_ROOT

print("Arguments passed:", sys.argv)

# Database connection setup
def connect_to_mssql():
    try:
        connection = pyodbc.connect(config.CONNECTION_STRING)
        #connection = pyodbc.connect(
        #    'DRIVER={ODBC Driver 17 for SQL Server};'
        #    'SERVER=10.32.1.51,1455;'
        #    'DATABASE=mis_db_prod_backup_2024_04_02;'
        #    'UID=mis_admin;'
        #    'PWD=Exim1234;'
        #    'Encrypt=yes;TrustServerCertificate=yes'  # Use if you encounter SSL issues
        #)
        print("Connected to MSSQL database successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to MSSQL database: {e}")
        sys.exit(f"Error connecting to MSSQL database: {str(e)}")
        #sys.exit(1)
        
#----------------------------------------------------------------------------------------------------

# Function to update user data
#def set_user(connection, documentId, documentName, jobName, statusName, uploadedById, uploadedByEmail, reportingDate):
#    print("Starting user update...")
#    try:
        # Open a cursor to interact with the database
#        with connection.cursor() as cursor:
            # Update the user data in the 'users' table
#            cursor.execute(
#                "UPDATE users SET username = ? WHERE userId = ?",
#                ('rozaimizamahriMISPYTHON', 1)
#            )
            # Commit the changes
#            connection.commit()
#        print("User updated successfully.")
#    except Exception as e:
#        print(f"Error updating user: {e}")
#        sys.exit(f"Error updating user: {str(e)}")
#        sys.exit(1)
        
#----------------------------------------------------------------------------------------------------

# Main function
if __name__ == "__main__":
    try:
        # Ensure we have the correct number of arguments
        if len(sys.argv) != 8:
            print("Usage: python testPython.py <documentId> <documentName> <jobName> <statusName> <uploadedById> <uploadedByEmail> <reportingDate>")
            sys.exit(1)

        # Parse command-line arguments
        documentId = int(sys.argv[1])
        documentName = sys.argv[2]
        jobName = sys.argv[3]
        statusName = sys.argv[4]
        uploadedById = int(sys.argv[5])
        uploadedByEmail = sys.argv[6]
        reportingDate = sys.argv[7] # YYYY-MM-DD

        print(f"Arguments received: {documentId}, {documentName}, {jobName}, {statusName}, {uploadedById}, {uploadedByEmail}, {reportingDate}")

        # Connect to MSSQL
        connection = connect_to_mssql()

        # Call the set_user function with the parsed arguments
        #set_user(connection, documentId, documentName, jobName, statusName, uploadedById, uploadedByEmail, reportingDate)

    except Exception as e:
        print(f"Script failed with exception: {e}")
        sys.exit(f"Script failed with exception: {str(e)}")
        sys.exit(1) # Exit the script with a failure code
    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
            print("Database connection closed.")
        
#----------------------------------------------------------------------------------------------------

try:
    #   Library
    import config
    import pandas as pd
    import numpy as np
    import pyodbc
    import datetime as dt
    #from sqlalchemy import create_engine
    #from sqlalchemy import Table, MetaData
    #from sqlalchemy import update
    #from sqlalchemy.orm import sessionmaker
    #import streamlit as st
    #import base64
    #from PIL import Image
    #import plotly.express as px

    #   Display
    #warnings.filterwarnings('ignore')
    pd.set_option("display.max_columns", None) 
    pd.set_option("display.max_colwidth", 1000) #huruf dlm column
    pd.set_option("display.max_rows", 100)
    pd.set_option("display.precision", 2) #2 titik perpuluhan

    #   Timestamp
    current_time = pd.Timestamp.now()
except Exception as e:
    print(f"Library Error: {e}")
    sys.exit(f"Library Error: {str(e)}")
    #sys.exit(1)
        
#----------------------------------------------------------------------------------------------------

try:
    #   pyodbc
    conn = pyodbc.connect(config.CONNECTION_STRING)
    #conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"+
    #                    "Server=10.32.1.51,1455;"+
    #                    "Database=mis_db_prod_backup_2024_04_02;"+
    #                    "Trusted_Connection=no;"+
    #                    "uid=mis_admin;"+
    #                    "pwd=Exim1234")
    cursor = conn.cursor()

    LDB_prev = pd.read_sql_query("SELECT * FROM col_facilities_application_master;", conn)
    
    sql_query1 = """UPDATE [jobPython]
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Debtor_Listing.py',[jobCompleted] = NULL,[jobErrDetail]=?
    WHERE [jobName] = 'Debtor Listing';
                """
    cursor.execute(sql_query1,os.getcwd())
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")
    sys.exit(f"Connect to Database Error: {str(e)}")
    #sys.exit(1)
        
#----------------------------------------------------------------------------------------------------

#process
try:
    BalanceOS = "Balance"

    #reportingDate = "2024-03-29"
    #data_folder = os.path.join(PROJECT_ROOT, "misPython_doc")
    df1 =  os.path.join(config.FOLDER_CONFIG["FTP_directory"],documentName)
    #df1 = r"D:\\mis_doc\\PythonProjects\\misPython\\misPython_doc\\Debtors Listing and Customer Balance Report as at October 2024_Adjusted.xlsx" 

    D1 = "Debtors Listing Islamic (Cost)"
    D2 = "Debtors Listing Islamic (Profit"
    D3 = "Modification MORA & R&R"
    D4 = "Debtors Listing (C)"
    D5 = "Accrued Interest"
    D6 = "Other Debtors Conv"
    D7 = "Other Debtors Islamic"
    D8 = "IIS"
    D9 = "PIS"
    D10 = "Penalty"
    D11 = "Ta'widh (Active)"
    D12 = "Ta'widh (Recovery)"

    Isl_Cost = pd.read_excel(df1, sheet_name=D1, header=5)
    #st.write(Isl_Cost.head(1))
    Isl_Profit = pd.read_excel(df1, sheet_name=D2, header=3)
    Mora = pd.read_excel(df1, sheet_name=D3, header=5)
    Conv = pd.read_excel(df1, sheet_name=D4, header=2)
    Accrued = pd.read_excel(df1, sheet_name=D5, header=4)
    Others_conv = pd.read_excel(df1, sheet_name=D6, header=4)
    Others_Isl = pd.read_excel(df1, sheet_name=D7, header=4)
    IIS = pd.read_excel(df1, sheet_name=D8, header=4)
    PIS = pd.read_excel(df1, sheet_name=D9, header=4)
    Penalty = pd.read_excel(df1, sheet_name=D10, header=4)
    Ta_A = pd.read_excel(df1, sheet_name=D11, header=4)
    Ta_R = pd.read_excel(df1, sheet_name=D12, header=4)
except Exception as e:
    print(f"Upload Excel Error: {e}")
    sql_query2 = """INSERT INTO [log_apps_error] (
                    [logerror_desc],
                    [iduser],
                    [dateerror],
                    [page],
                    [user_name]
                )
                VALUES
                    (?,  
                    0,  
                    getdate(),  
                    ?,  
                    ?
                    )
                """
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","Debtor Listing",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Upload Excel Debtor Listing'
    WHERE [jobName] = 'Debtor Listing';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Upload Excel Debtor Listing Error: {e}")
    sys.exit(f"Upload Excel Debtor Listing Error: {str(e)}")
    #sys.exit(1) 

    #==============================================================================================

    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Not Applicable",'PY003','PY003')] #,36961,36961
    download_error = pd.DataFrame(data,columns=columns)
    
    # Assuming 'combine2' is a DataFrame
    column_types1 = []
    for col in download_error.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if download_error[col].dtype == 'object':  # String data type
            column_types1.append(f"{col} VARCHAR(255)")
        elif download_error[col].dtype == 'int64':  # Integer data type
            column_types1.append(f"{col} INT")
        elif download_error[col].dtype == 'float64':  # Float data type
            column_types1.append(f"{col} FLOAT")
        else:
            column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

    create_table_query_result = "CREATE TABLE A_download_error (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_error.iterrows():
        sql_result = "INSERT INTO A_download_error({}) VALUES ({})".format(','.join(download_error.columns), ','.join(['?']*len(download_error.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_error AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);    
    """)
    conn.commit() 
    cursor.execute("drop table A_download_error")
    conn.commit() 

#------------------------------------------------------------------------------------------------

try:
    #---------------------------------Debtors Listing Islamic (Cost) include adjustment
    Isl_Cost1 = Isl_Cost.iloc[np.where(~Isl_Cost['Customer\nAccount'].isna())]

    Isl_Cost1.columns = Isl_Cost1.columns.str.replace("\n", "_")
    Isl_Cost1.columns = Isl_Cost1.columns.str.replace(" ", "")

    Isl_Cost1.Customer_Account = Isl_Cost1.Customer_Account.astype(int)
    Isl_Cost1.Disbursement = Isl_Cost1.Disbursement.astype(float)
    Isl_Cost1.Cost_Payment = Isl_Cost1.Cost_Payment.astype(float)
    Isl_Cost1[BalanceOS] = Isl_Cost1[BalanceOS].astype(float)

    Isl_Cost1.rename(columns={"Disbursement":"Disbursement - old"}, inplace=True)
    Isl_Cost1['Disbursement'] = Isl_Cost1['Disbursement - old'].fillna(0)# + Isl_Cost1['Adjustment/_Capitalisation'].fillna(0)

    Isl_Cost1.rename(columns={"Cost_Payment":"Cost_Payment - old"}, inplace=True)
    Isl_Cost1['Cost_Payment'] = Isl_Cost1['Cost_Payment - old'].fillna(0)# - Isl_Cost1['Adjustment/_Capitalisation.1'].fillna(0)

    Isl_Cost2 = Isl_Cost1.fillna(0).groupby(['Company','Customer_Account'\
    ,'Currency'])[['Disbursement'\
    ,'Cost_Payment',BalanceOS]].sum().reset_index()

    Isl_Cost2 = Isl_Cost2.rename(columns={BalanceOS: 'Principal'}).fillna(0).sort_values(by=['Principal'],ascending=[True])
    #Isl_Cost2['Sheet'] = 'Debtors Listing Islamic (Cost)'
    Isl_Cost2['Financing_Type'] = 'Islamic'

    #---------------------------------Debtors Listing Islamic (Profit) include adjustment

    Isl_Profit1 = Isl_Profit.iloc[np.where(~Isl_Profit['Customer\nAccount'].isna())]

    Isl_Profit1.columns = Isl_Profit1.columns.str.replace("\n", "_")
    Isl_Profit1.columns = Isl_Profit1.columns.str.replace(" ", "")

    Isl_Profit1.Customer_Account = Isl_Profit1.Customer_Account.astype(int)
    Isl_Profit1.Unearned_Profit = Isl_Profit1.Unearned_Profit.astype(float)
    Isl_Profit1.Profit_Payment = Isl_Profit1.Profit_Payment.astype(float)
    Isl_Profit1[BalanceOS] = Isl_Profit1[BalanceOS].astype(float)

    Isl_Profit1.rename(columns={"Profit_Payment":"Profit_Payment - old"}, inplace=True)
    Isl_Profit1['Profit_Payment'] = Isl_Profit1['Profit_Payment - old'].fillna(0)# - Isl_Profit1['Adjustment/_Capitalisation.1'].fillna(0)

    Isl_Profit2 = Isl_Profit1.fillna(0).groupby(['Company','Customer_Account'\
    ,'Currency'])[['Unearned_Profit','Rental(Ijarah)','Profit_Payment',BalanceOS]].sum().reset_index()

    Isl_Profit2 = Isl_Profit2.rename(columns={BalanceOS: 'Interest'}).fillna(0).sort_values(by=['Interest'],ascending=[True])
    #Isl_Profit2['Sheet'] = 'Debtors Listing Islamic (Profit)'
    Isl_Profit2['Financing_Type'] = 'Islamic'

    #Combine Islamic Cost+Profit
    A001 = Isl_Cost2.merge(Isl_Profit2,on=['Customer_Account','Company','Currency','Financing_Type'],how='outer',indicator=True)

    A001 = A001.drop(columns=['_merge'])

    A002 = A001.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type'])[['Disbursement'\
    ,'Cost_Payment','Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest']].sum().reset_index()

    NamaCompany = A001[['Company','Customer_Account']].drop_duplicates('Customer_Account', keep='first')
    A003 = A002.merge(NamaCompany,on='Customer_Account',how='left')

    #---------------------------------Modification MORA & R&R Apr2024

    Mora1 = Mora.fillna(0).rename(columns={'Borrower code': 'Customer_Account',
                            'Borrower':'Company',
                            'Modification impact (RM)':'Mora',
                            'Islamic/ conventional':'Financing_Type'}).iloc[np.where((~Mora['Borrower code'].isna())&(Mora['Borrower code']!='Borrower code'))]

    Mora1.columns = Mora1.columns.str.replace("\n", "_")
    Mora1.columns = Mora1.columns.str.replace(" ", "")

    Mora1.Customer_Account = Mora1.Customer_Account.astype(int)
    Mora1.Mora = Mora1.Mora.astype(float)

    Mora1.loc[Mora1.Currency.isin(['RM']),'Currency'] = 'MYR'

    A004 = A003.merge(Mora1[['Customer_Account','Company','Currency','Financing_Type','SLOacceptancedate','Mora']],on=['Customer_Account','Company','Currency','Financing_Type'],how='outer',indicator=True)

    NamaMora = A004[['Company','Customer_Account']].drop_duplicates('Customer_Account', keep='first')
    A004 = A004.drop(['Company','_merge'],axis=1).merge(NamaMora,on='Customer_Account',how='left')

    A004 = A004.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Cost_Payment','Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest','Mora']].sum().reset_index()

    #---------------------------------Other Debtors Islamic Apr2024

    Others_Isl1 = Others_Isl.iloc[np.where(~Others_Isl.Customer.isna())].fillna(0)

    Others_Isl1.columns = Others_Isl1.columns.str.replace("\n", "_")
    Others_Isl1.columns = Others_Isl1.columns.str.replace(" ", "")

    Others_Isl1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Other_Charges'},inplace=True)

    Others_Isl1.Customer_Account = Others_Isl1.Customer_Account.astype(int)
    Others_Isl1.Other_Charges = Others_Isl1.Other_Charges.astype(float)

    Others_Isl1 = Others_Isl1.fillna(0).groupby(['Company','Customer_Account'])[['Other_Charges']].sum().reset_index()

    Others_Isl1['Financing_Type'] = 'Islamic'

    A005 = A004.merge(Others_Isl1,on=['Customer_Account','Company','Financing_Type'],how='outer',indicator=True)

    NamaOther = A005[['Company','Customer_Account','Currency']].drop_duplicates('Customer_Account', keep='first')
    A005 = A005.drop(['Company','Currency','_merge'],axis=1).merge(NamaOther,on='Customer_Account',how='left')

    A005 = A005.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Cost_Payment','Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest','Mora','Other_Charges']].sum().reset_index()

    #---------------------------------PIS

    PIS1 = PIS.iloc[np.where(~PIS.Customer.isna())].fillna(0)

    PIS1.columns = PIS1.columns.str.replace("\n", "_")
    PIS1.columns = PIS1.columns.str.replace(" ", "")

    PIS1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Interest_in_Suspense'},inplace=True)

    PIS1.Customer_Account = PIS1.Customer_Account.astype(int)
    PIS1.Interest_in_Suspense = PIS1.Interest_in_Suspense.astype(float)

    PIS1 = PIS1.fillna(0).groupby(['Company','Customer_Account'])[['Interest_in_Suspense']].sum().reset_index()

    PIS1['Financing_Type'] = 'Islamic'

    A006 = A005.merge(PIS1,on=['Customer_Account','Company','Financing_Type'],how='outer',indicator=True)

    NamaPIS = A006[['Company','Customer_Account','Currency']].drop_duplicates('Customer_Account', keep='first')
    A006 = A006.drop(['Company','Currency','_merge'],axis=1).merge(NamaPIS,on='Customer_Account',how='left')

    A006 = A006.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Cost_Payment','Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest','Mora','Other_Charges','Interest_in_Suspense']].sum().reset_index()

    #---------------------------------Penalty Islamic

    Ta_A1 = Ta_A.iloc[np.where(~Ta_A.Customer.isna())].fillna(0)

    Ta_A1.columns = Ta_A1.columns.str.replace("\n", "_")
    Ta_A1.columns = Ta_A1.columns.str.replace(" ", "")

    Ta_A1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Penalty_Tawidh'},inplace=True)

    Ta_A1.Customer_Account = Ta_A1.Customer_Account.astype(int)
    Ta_A1.Penalty_Tawidh = Ta_A1.Penalty_Tawidh.astype(float)

    Ta_A1 = Ta_A1.fillna(0).groupby(['Company','Customer_Account'])[['Penalty_Tawidh']].sum().reset_index()

    Ta_A1['Financing_Type'] = 'Islamic'

    Ta_R1 = Ta_R.iloc[np.where(~Ta_R.Customer.isna())].fillna(0)

    Ta_R1.columns = Ta_R1.columns.str.replace("\n", "_")
    Ta_R1.columns = Ta_R1.columns.str.replace(" ", "")

    Ta_R1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Recovery_Tawidh'},inplace=True)

    Ta_R1.Customer_Account = Ta_R1.Customer_Account.astype(int)
    Ta_R1.Recovery_Tawidh = Ta_R1.Recovery_Tawidh.astype(float)

    Ta_R1 = Ta_R1.fillna(0).groupby(['Company','Customer_Account'])[['Recovery_Tawidh']].sum().reset_index()

    Ta_R1['Financing_Type'] = 'Islamic'

    #Ta_A1.columns = Ta_R1.columns
    Ta_AR = pd.concat([Ta_A1,Ta_R1])
    Ta_AR.fillna(0, inplace=True)

    #st.write(Ta_AR)

    A006_1 = A006.merge(Ta_AR,on=['Customer_Account','Company','Financing_Type'],how='outer',indicator=True)

    NamaTa_AR = A006_1[['Company','Customer_Account','Currency']].drop_duplicates('Customer_Account', keep='first')
    A006_1 = A006_1.drop(['Company','Currency','_merge'],axis=1).merge(NamaTa_AR,on='Customer_Account',how='left')

    A006_1 = A006_1.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Cost_Payment','Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest','Mora','Other_Charges','Interest_in_Suspense',"Penalty_Tawidh",'Recovery_Tawidh']].sum().reset_index()

    #st.write(sum(A006_1["Penalty_Tawidh"]))
    #st.write(sum(Ta_AR["Penalty_Tawidh"]))

    #-------------------------------------------------conv--------------------------------------------------

    #Debtors Listing Conv Apr 2024
    Conv1 = Conv.iloc[np.where(~Conv['Customer Account Number'].isna())]

    Conv1.columns = Conv1.columns.str.replace("\n", "_")
    Conv1.columns = Conv1.columns.str.replace(" ", "")

    Conv1 = Conv1.rename(columns={'CustomerAccountNumber': 'Customer_Account',
                            'CustomerName':'Company',
                            'LoanCurrency':'Currency',
                            'ClosingPrincipal':'Principal'}).fillna(0)

    Conv1.Customer_Account = Conv1.Customer_Account.astype(int)
    Conv1.Principal = Conv1.Principal.astype(float)

    #[['Customer_Account','Company','Currency','Disbursement','Repayment','Principal']]

    Conv1.rename(columns={"Disbursement":"Disbursement - old"}, inplace=True)
    Conv1['Disbursement'] = Conv1['Disbursement - old'].fillna(0)# + Conv1['AdjustmentCapitalization'].fillna(0)

    Conv1.rename(columns={"Repayment":"Repayment - old"}, inplace=True)
    Conv1['Repayment'] = Conv1['Repayment - old'].fillna(0)# - Conv1['AdjustmentCapitalization.1'].fillna(0)

    Conv1 = Conv1.fillna(0).groupby(['Company','Customer_Account'\
    ,'Currency'])[['Disbursement'\
    ,'Repayment','Principal']].sum().reset_index()

    Conv1['Financing_Type'] = 'Conventional'

    #---------------------------------Accrued Interest Apr2024
    Accrued1 = Accrued.iloc[np.where(~Accrued.Customer.isna())].fillna(0)

    Accrued1.columns = Accrued1.columns.str.replace("\n", "_")
    Accrued1.columns = Accrued1.columns.str.replace(" ", "")

    Accrued1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Interest',
                            'Debitrept.period':'Interest_For_the_Month',
                            'Creditreportper.':'Profit_Payment'},inplace=True)

    Accrued1.Customer_Account = Accrued1.Customer_Account.astype(int)
    Accrued1.Interest = Accrued1.Interest.astype(float)
    Accrued1.Interest_For_the_Month = Accrued1.Interest_For_the_Month.astype(float)
    Accrued1.Profit_Payment = Accrued1.Profit_Payment.astype(float)

    Accrued1.loc[(Accrued1['SGLInd.'].isin(['X'])),'Interest_For_the_Month'] = Accrued1.Interest_For_the_Month
    Accrued1.loc[(~Accrued1['SGLInd.'].isin(['X'])),'Interest_For_the_Month'] = 0

    Accrued1.loc[(Accrued1['SGLInd.'].isin(['X'])),'Profit_Payment'] = Accrued1.Profit_Payment
    Accrued1.loc[(~Accrued1['SGLInd.'].isin(['X'])),'Profit_Payment'] = 0

    Accrued1 = Accrued1.fillna(0).groupby(['Company','Customer_Account'])[['Interest_For_the_Month','Interest','Profit_Payment']].sum().reset_index()

    Accrued1['Financing_Type'] = 'Conventional'

    #Combine Conv Principal+Accrued
    C001 = Conv1.merge(Accrued1,on=['Customer_Account','Company','Financing_Type'],how='outer',indicator=True)

    NamaConv = C001[['Company','Customer_Account','Currency']].drop_duplicates('Customer_Account', keep='first')
    C002 = C001.drop(['Company','Currency','_merge'],axis=1).merge(NamaConv,on='Customer_Account',how='left')

    C002 = C002.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Repayment','Principal','Interest_For_the_Month','Interest','Profit_Payment']].sum().reset_index()

    #Other Debtors Apr2024
    Others_conv1 = Others_conv.iloc[np.where(~Others_conv.Customer.isna())].fillna(0)

    Others_conv1.columns = Others_conv1.columns.str.replace("\n", "_")
    Others_conv1.columns = Others_conv1.columns.str.replace(" ", "")

    Others_conv1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Other_Charges'},inplace=True)

    Others_conv1.Customer_Account = Others_conv1.Customer_Account.astype(int)
    Others_conv1.Other_Charges = Others_conv1.Other_Charges.astype(float)

    Others_conv1 = Others_conv1.fillna(0).groupby(['Company','Customer_Account'])[['Other_Charges']].sum().reset_index()

    Others_conv1['Financing_Type'] = 'Conventional'

    C003 = C002.merge(Others_conv1,on=['Customer_Account','Company','Financing_Type'],how='outer',indicator=True)

    NamaOtherConv = C003[['Company','Customer_Account','Currency']].drop_duplicates('Customer_Account', keep='first')
    C004 = C003.drop(['Company','Currency','_merge'],axis=1).merge(NamaOtherConv,on='Customer_Account',how='left')

    C004 = C004.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Repayment','Principal','Interest_For_the_Month','Interest','Profit_Payment','Other_Charges']].sum().reset_index()

    #IIS
    IIS1 = IIS.iloc[np.where(~IIS.Customer.isna())].fillna(0)

    IIS1.columns = IIS1.columns.str.replace("\n", "_")
    IIS1.columns = IIS1.columns.str.replace(" ", "")

    IIS1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Interest_in_Suspense'},inplace=True)

    IIS1.Customer_Account = IIS1.Customer_Account.astype(int)
    IIS1.Interest_in_Suspense = IIS1.Interest_in_Suspense.astype(float)

    IIS1 = IIS1.fillna(0).groupby(['Company','Customer_Account'])[['Interest_in_Suspense']].sum().reset_index()

    IIS1['Financing_Type'] = 'Conventional'

    C005 = C004.merge(IIS1,on=['Customer_Account','Company','Financing_Type'],how='outer', indicator=True)

    NamaIIS = C005[['Company','Customer_Account','Currency']].drop_duplicates('Customer_Account', keep='first')
    C005 = C005.drop(['Company','Currency','_merge'],axis=1).merge(NamaIIS,on='Customer_Account',how='left')

    C005 = C005.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Repayment','Principal','Interest_For_the_Month','Interest','Profit_Payment','Other_Charges','Interest_in_Suspense']].sum().reset_index()

    #---------------------------------Penalty Conventional

    Penalty1 = Penalty.iloc[np.where(~Penalty.Customer.isna())].fillna(0)

    Penalty1.columns = Penalty1.columns.str.replace("\n", "_")
    Penalty1.columns = Penalty1.columns.str.replace(" ", "")

    Penalty1.rename(columns={'Customer': 'Customer_Account',
                            'SearchTerm':'Company',
                            'Crcy':'Currency',
                            'Accumulatedbalance':'Penalty_Tawidh'},inplace=True)

    Penalty1.Customer_Account = Penalty1.Customer_Account.astype(int)
    Penalty1.Penalty_Tawidh = Penalty1.Penalty_Tawidh.astype(float)

    Penalty1 = Penalty1.fillna(0).groupby(['Company','Customer_Account'])[['Penalty_Tawidh']].sum().reset_index()

    Penalty1['Financing_Type'] = 'Conventional'


    C005_1 = C005.merge(Penalty1,on=['Customer_Account','Company','Financing_Type'],how='outer', indicator=True)

    NamaPenal = C005_1[['Company','Customer_Account','Currency']].drop_duplicates('Customer_Account', keep='first')
    C005_1 = C005_1.drop(['Company','Currency','_merge'],axis=1).merge(NamaPenal,on='Customer_Account',how='left')

    C005_1 = C005_1.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Repayment','Principal','Interest_For_the_Month','Interest','Profit_Payment','Other_Charges','Interest_in_Suspense',"Penalty_Tawidh"]].sum().reset_index()

    #st.write(sum(C005_1["Penalty_Tawidh"]))
    #st.write(sum(Penalty1["Penalty_Tawidh"]))

    #-------------------------------------------------combine-------------------------------------------------
    C005_1['Recovery_Tawidh'] = 0
    C005_1['Cost_Payment'] = 0
    C005_1['Unearned_Profit'] = C005['Interest_For_the_Month']
    #C005['Profit_Payment'] = 0
    C005_1['Mora'] = 0
    C005_1['Rental(Ijarah)'] = 0

    C006 = C005_1[['Customer_Account','Currency','Financing_Type','Company','Disbursement','Repayment','Cost_Payment',
                'Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest','Mora','Other_Charges',
                'Interest_in_Suspense',"Penalty_Tawidh",'Recovery_Tawidh']]

    A006_1['Repayment'] = 0

    A007 = A006_1[['Customer_Account','Currency','Financing_Type','Company','Disbursement','Repayment','Cost_Payment',
                'Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest','Mora','Other_Charges',
                'Interest_in_Suspense',"Penalty_Tawidh",'Recovery_Tawidh']]

    #Isl_Cost1.columns = Isl_Profit1.columns = Mora1.columns = Conv1.columns
    #appendR = pd.concat([Isl_Cost1,Isl_Profit1,Mora1,Conv1] )

    C006.columns = A007.columns
    appendR = pd.concat([C006,A007])

    NamaappendR = appendR.iloc[np.where(~(appendR.Currency.isin([0,'0'])))][['Company','Currency','Customer_Account']].drop_duplicates('Customer_Account', keep='first')
    appendR = appendR.drop(['Company','Currency'],axis=1).merge(NamaappendR,on='Customer_Account',how='left')

    appendfinal = appendR.fillna(0).groupby(['Customer_Account'\
    ,'Currency','Financing_Type','Company'])[['Disbursement'\
    ,'Repayment','Cost_Payment','Principal','Unearned_Profit','Rental(Ijarah)','Profit_Payment','Interest','Mora','Other_Charges','Interest_in_Suspense',"Penalty_Tawidh",'Recovery_Tawidh']].sum().reset_index()

    appendfinal['Total Loans Outstanding (MYR)'] = appendfinal['Principal'] + appendfinal['Interest'] + appendfinal['Mora'] + appendfinal['Other_Charges'] + appendfinal['Penalty_Tawidh']

    appendfinal['Cost Payment/Principal Repayment (MYR)'] = (-1*appendfinal['Repayment']) + appendfinal['Cost_Payment']
    appendfinal['Accrued Profit/Interest of the month (MYR)'] = appendfinal['Unearned_Profit'] + appendfinal['Rental(Ijarah)'] #+ profit for the month

    appendfinal.rename(columns={'Customer_Account':'finance_sap_number',
                            'Currency':'Facility Currency',
                            'Financing_Type':'Type of Financing',
                            "Company":"Customer Name",
                            "Disbursement":"Disbursement/Drawdown (MYR)",
                            'Principal':'Cost/Principal Outstanding (MYR)',
                            'Profit_Payment':"Profit Payment/Interest Repayment (MYR)",
                            'Interest':'Cumulative Accrued Profit/Interest (MYR)',
                            'Mora':'Modification of Loss (MYR)',
                            'Other_Charges':'Other Charges (MYR)',
                            'Interest_in_Suspense':'Income/Interest in Suspense (MYR)',
                            "Penalty_Tawidh":"Ta`widh Payment/Penalty Repayment (MYR)",
                            'Recovery_Tawidh':"Ta'widh (Compensation) (MYR)"}, inplace=True)

    appendfinal.drop(columns=['Repayment','Cost_Payment','Unearned_Profit','Rental(Ijarah)'], axis=1, inplace=True)

                            #"Repayment":"1. Cost Payment/Principal Repayment (MYR)",
                            #"Cost_Payment":'2. Cost Payment/Principal Repayment (MYR)',


                            #'Unearned_Profit':'Accrued Profit/Interest of the month (MYR)',
                            #'Rental(Ijarah)':'Ijarah',
    appendfinal['finance_sap_number'] = appendfinal['finance_sap_number'].astype(str)
    
    LDB_prev['finance_sap_number'] = LDB_prev['finance_sap_number'].astype(str)
    LDB_prev.columns = LDB_prev.columns.str.replace("\n", "")

    appendfinal_ldb = appendfinal.merge(LDB_prev.iloc[np.where(~LDB_prev['finance_sap_number'].isna())][['finance_sap_number',
                                                'facility_amount_outstanding',
                                                'acc_principal_amount_outstanding',
                                                'acc_accrued_interest_month_fc',
                                                'acc_accrued_interest_month_myr',
                                                'acc_modification_loss',
                                                'acc_modification_loss_myr',
                                                'acc_accurate_interest',
                                                'acc_accrued_interest_myr',
                                                'acc_suspended_interest',
                                                'acc_interest_suspense_myr',
                                                'acc_other_charges',
                                                'acc_other_charges_myr',
                                                'acc_penalty',
                                                'acc_penalty_myr',
                                                'acc_penalty_compensation_fc',
                                                'acc_penalty_compensation_myr',
                                                'acc_balance_outstanding_fc',
                                                'acc_balance_outstanding_myr',
                                                'acc_drawdown_myr',
                                                'acc_repayment_myr']].drop_duplicates('finance_sap_number',keep='first'),on=['finance_sap_number'],how='inner', suffixes=('_x', ''),indicator=True)



    #appendfinal_ldb['Facility Currency'] = appendfinal_ldb['Facility Currency'].astype(str)
    #appendfinal_ldb['Facility Currency'] = appendfinal_ldb['Facility Currency'].str.strip()
    Currency = pd.read_sql_query("""Select finance_sap_number
    ,b.param_name as currency
    from col_facilities_application_master a
    left outer join param_system_param b on a.facility_ccy_id = b.param_id;""", conn)

    Currency['finance_sap_number'] = Currency['finance_sap_number'].astype(str)
    Currency.columns = Currency.columns.str.replace("\n", "")

    aa = pd.read_sql_query("""SELECT param_name,r.exchange_rate,r.valuedate
    FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
    order by param_name asc;""", conn)
    
    MRate = aa.iloc[np.where(aa.valuedate==reportingDate)]

    df_add = pd.DataFrame([['MYR',
                        '1',
                        reportingDate]], columns=['param_name','exchange_rate','valuedate'])

    MRate1 = pd.concat([MRate, df_add])
    
    appendfinal2 = appendfinal_ldb.merge(Currency, on='finance_sap_number', how='left').\
    merge(MRate1.rename(columns={'param_name':'currency'}), on="currency",how='left') #

    appendfinal2['exchange_rate'] = appendfinal2['exchange_rate'].astype(float)

    appendfinal2['Cost/Principal Outstanding (Facility Currency)'] = appendfinal2['Cost/Principal Outstanding (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Accrued Profit/Interest of the month (Facility Currency)'] = appendfinal2['Accrued Profit/Interest of the month (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Modification of Loss (Facility Currency)'] = appendfinal2['Modification of Loss (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Cumulative Accrued Profit/Interest (Facility Currency)'] = appendfinal2['Cumulative Accrued Profit/Interest (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Income/Interest in Suspense (Facility Currency)'] = appendfinal2['Income/Interest in Suspense (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Other Charges (Facility Currency)'] = appendfinal2['Other Charges (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Total Loans Outstanding (Facility Currency)'] = appendfinal2['Total Loans Outstanding (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Disbursement/Drawdown (Facility Currency)'] = appendfinal2['Disbursement/Drawdown (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Cost Payment/Principal Repayment (Facility Currency)'] = appendfinal2['Cost Payment/Principal Repayment (MYR)']/appendfinal2['exchange_rate']
    appendfinal2['Profit Payment/Interest Repayment (Facility Currency)'] = appendfinal2['Profit Payment/Interest Repayment (MYR)']/appendfinal2['exchange_rate']
    appendfinal2["Ta`widh Payment/Penalty Repayment (Facility Currency)"] = appendfinal2["Ta`widh Payment/Penalty Repayment (MYR)"]/appendfinal2['exchange_rate']
    appendfinal2["Ta'widh (Compensation) (Facility Currency)"] = appendfinal2["Ta'widh (Compensation) (MYR)"]/appendfinal2['exchange_rate']


    #appendfinal2['Cumulative Disbursement/Drawdown (Facility Currency) New'] = appendfinal2['Disbursement/Drawdown (Facility Currency)'] +  appendfinal2['Cumulative Disbursement/Drawdown (Facility Currency)'] 
    #appendfinal2['Cumulative Disbursement/Drawdown (MYR) New'] = appendfinal2['Disbursement/Drawdown (MYR)'] +  appendfinal2['Cumulative Disbursement/Drawdown (MYR)'] 
    #appendfinal2['Cumulative Cost Payment/Principal Repayment (Facility Currency) New'] = appendfinal2['Cost Payment/Principal Repayment (Facility Currency)'] +  appendfinal2['Cumulative Cost Payment/Principal Repayment (Facility Currency)'] 
    #appendfinal2['Cumulative Cost Payment/Principal Repayment (MYR) New'] = appendfinal2['Cost Payment/Principal Repayment (MYR)'] +  appendfinal2['Cumulative Cost Payment/Principal Repayment (MYR)'] 
    #appendfinal2['Cumulative Profit Payment/Interest Repayment (Facility Currency) New'] = appendfinal2['Profit Payment/Interest Repayment (Facility Currency)'] +  appendfinal2['Cumulative Profit Payment/Interest Repayment (Facility Currency)'] 
    #appendfinal2['Cumulative Profit Payment/Interest Repayment (MYR) New'] = appendfinal2['Profit Payment/Interest Repayment (MYR)'] +  appendfinal2['Cumulative Profit Payment/Interest Repayment (MYR)'] 

    appendfinal2.sort_values('Total Loans Outstanding (MYR)', ascending=False, inplace=True)#.reset_index()

    appendfinal3 = appendfinal2[['finance_sap_number',
                                #'Customer Name',
                                #'Facility Currency',
                                #'Type of Financing',
                                'Cost/Principal Outstanding (Facility Currency)',
                                'Cost/Principal Outstanding (MYR)',
                                'Accrued Profit/Interest of the month (Facility Currency)',
                                'Accrued Profit/Interest of the month (MYR)',
                                'Modification of Loss (Facility Currency)',
                                'Modification of Loss (MYR)',
                            'Cumulative Accrued Profit/Interest (Facility Currency)',
                            'Cumulative Accrued Profit/Interest (MYR)', 
                                'Income/Interest in Suspense (Facility Currency)',
                                'Income/Interest in Suspense (MYR)',
                                'Other Charges (Facility Currency)',
                                'Other Charges (MYR)',
                                "Ta`widh Payment/Penalty Repayment (Facility Currency)",
                                "Ta`widh Payment/Penalty Repayment (MYR)",
                                "Ta'widh (Compensation) (Facility Currency)",
                                "Ta'widh (Compensation) (MYR)",
                                'Total Loans Outstanding (Facility Currency)',
                                'Total Loans Outstanding (MYR)',
                            #'Disbursement/Drawdown (Facility Currency)',
                            'Disbursement/Drawdown (MYR)',
                                #'Cumulative Disbursement/Drawdown (Facility Currency) New',
                                #'Cumulative Disbursement/Drawdown (Facility Currency)',
                            #'Cumulative Disbursement/Drawdown (MYR) New',
                                #'Cumulative Disbursement/Drawdown (MYR)',
                            #'Cost Payment/Principal Repayment (Facility Currency)',
                            'Cost Payment/Principal Repayment (MYR)']]#,
                                #'Cumulative Cost Payment/Principal Repayment (Facility Currency) New',
                                #'Cumulative Cost Payment/Principal Repayment (Facility Currency)',
                            #'Cumulative Cost Payment/Principal Repayment (MYR) New',
                                #'Cumulative Cost Payment/Principal Repayment (MYR)',
                            #'Profit Payment/Interest Repayment (Facility Currency)',
                            #'Profit Payment/Interest Repayment (MYR)',
                                #'Cumulative Profit Payment/Interest Repayment (Facility Currency) New',
                                #'Cumulative Profit Payment/Interest Repayment (Facility Currency)',
                                #'Cumulative Profit Payment/Interest Repayment (MYR) New',
                                #'Cumulative Profit Payment/Interest Repayment (MYR)',
                                #'exchange_rate']]

    appendfinal3['Cost/Principal Outstanding (Facility Currency)'].fillna(0,inplace=True)
    appendfinal3['Cost/Principal Outstanding (MYR)'].fillna(0,inplace=True)
    appendfinal3['Accrued Profit/Interest of the month (Facility Currency)'].fillna(0,inplace=True)
    appendfinal3['Accrued Profit/Interest of the month (MYR)'].fillna(0,inplace=True)
    appendfinal3['Modification of Loss (Facility Currency)'].fillna(0,inplace=True)
    appendfinal3['Modification of Loss (MYR)'].fillna(0,inplace=True)
    appendfinal3['Cumulative Accrued Profit/Interest (Facility Currency)'].fillna(0,inplace=True)
    appendfinal3['Cumulative Accrued Profit/Interest (MYR)'].fillna(0,inplace=True)
    appendfinal3['Income/Interest in Suspense (Facility Currency)'].fillna(0,inplace=True)
    appendfinal3['Income/Interest in Suspense (MYR)'].fillna(0,inplace=True)
    appendfinal3['Other Charges (Facility Currency)'].fillna(0,inplace=True)
    appendfinal3['Other Charges (MYR)'].fillna(0,inplace=True)
    appendfinal3["Ta`widh Payment/Penalty Repayment (Facility Currency)"].fillna(0,inplace=True)
    appendfinal3["Ta`widh Payment/Penalty Repayment (MYR)"].fillna(0,inplace=True)
    appendfinal3["Ta'widh (Compensation) (Facility Currency)"].fillna(0,inplace=True)
    appendfinal3["Ta'widh (Compensation) (MYR)"].fillna(0,inplace=True)
    appendfinal3['Total Loans Outstanding (Facility Currency)'].fillna(0,inplace=True)
    appendfinal3['Total Loans Outstanding (MYR)'].fillna(0,inplace=True)

    appendfinal3["Disbursement/Drawdown (MYR)"].fillna(0,inplace=True)
    #appendfinal3["Cumulative Disbursement/Drawdown (MYR) New"].fillna(0,inplace=True)
    appendfinal3['Cost Payment/Principal Repayment (MYR)'].fillna(0,inplace=True)
    #appendfinal3['Cumulative Cost Payment/Principal Repayment (MYR) New'].fillna(0,inplace=True)

    appendfinal3.rename(columns={'Cost/Principal Outstanding (Facility Currency)':'facility_amount_outstanding',
                                'Cost/Principal Outstanding (MYR)':'acc_principal_amount_outstanding',
                                'Accrued Profit/Interest of the month (Facility Currency)':'acc_accrued_interest_month_fc',
                                'Accrued Profit/Interest of the month (MYR)':'acc_accrued_interest_month_myr',
                                'Modification of Loss (Facility Currency)':'acc_modification_loss',
                                'Modification of Loss (MYR)':'acc_modification_loss_myr',
                                'Cumulative Accrued Profit/Interest (Facility Currency)':'acc_accurate_interest',
                                'Cumulative Accrued Profit/Interest (MYR)':'acc_accrued_interest_myr',
                                'Income/Interest in Suspense (Facility Currency)':'acc_suspended_interest',
                                'Income/Interest in Suspense (MYR)':'acc_interest_suspense_myr',
                                'Other Charges (Facility Currency)':'acc_other_charges',
                                'Other Charges (MYR)':'acc_other_charges_myr',
                                "Ta`widh Payment/Penalty Repayment (Facility Currency)":'acc_penalty',
                                "Ta`widh Payment/Penalty Repayment (MYR)":'acc_penalty_myr',
                                "Ta'widh (Compensation) (Facility Currency)":'acc_penalty_compensation_fc',
                                "Ta'widh (Compensation) (MYR)":'acc_penalty_compensation_myr',
                                'Total Loans Outstanding (Facility Currency)':'acc_balance_outstanding_fc',
                                'Total Loans Outstanding (MYR)':'acc_balance_outstanding_myr',
                                'Disbursement/Drawdown (MYR)':'acc_drawdown_myr',
                                'Cost Payment/Principal Repayment (MYR)':'acc_repayment_myr'},inplace=True)
    appendfinal3.drop_duplicates('finance_sap_number',keep='first',inplace=True)

    df_add_Humm = pd.DataFrame([['500776A',
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0]], columns=['finance_sap_number',
                                              'facility_amount_outstanding',
                                              'acc_principal_amount_outstanding',
                                              'acc_accrued_interest_month_fc',
                                              'acc_accrued_interest_month_myr',
                                              'acc_modification_loss',
                                              'acc_modification_loss_myr',
                                              'acc_accurate_interest',
                                              'acc_accrued_interest_myr',
                                              'acc_suspended_interest',
                                              'acc_interest_suspense_myr',
                                              'acc_other_charges',
                                              'acc_other_charges_myr',
                                              'acc_penalty',
                                              'acc_penalty_myr',
                                              'acc_penalty_compensation_fc',
                                              'acc_penalty_compensation_myr',
                                              'acc_balance_outstanding_fc',
                                              'acc_balance_outstanding_myr',
                                              'acc_drawdown_myr',
                                              'acc_repayment_myr'])

    appendfinal3 = pd.concat([appendfinal3, df_add_Humm])


    #humming bird
    a_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['facility_amount_outstanding'])
    b_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_principal_amount_outstanding'])
    c_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_accrued_interest_month_fc'])
    d_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_accrued_interest_month_myr'])
    e_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_modification_loss'])
    f_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_modification_loss_myr'])
    g_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_accurate_interest'])
    h_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_accrued_interest_myr'])
    i_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_suspended_interest'])
    j_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_interest_suspense_myr'])
    k_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_other_charges'])
    l_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_other_charges_myr'])
    m_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_penalty'])
    n_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_penalty_myr'])
    o_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_penalty_compensation_fc'])
    p_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_penalty_compensation_myr'])
    q_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_balance_outstanding_fc'])
    r_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_balance_outstanding_myr'])
    s_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_drawdown_myr'])
    t_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['finance_sap_number']=='500776')]['acc_repayment_myr'])

    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'facility_amount_outstanding'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_principal_amount_outstanding'] = 0.79*b_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_accrued_interest_month_fc'] = 0.79*c_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_accrued_interest_month_myr'] = 0.79*d_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_modification_loss'] = 0.79*e_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_modification_loss_myr'] = 0.79*f_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_accurate_interest'] = 0.79*g_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_accrued_interest_myr'] = 0.79*h_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_suspended_interest'] = 0.79*i_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_interest_suspense_myr'] = 0.79*j_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_other_charges'] = 0.79*k_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_other_charges_myr'] = 0.79*l_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_penalty'] = 0.79*m_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_penalty_myr'] = 0.79*n_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_penalty_compensation_fc'] = 0.79*o_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_penalty_compensation_myr'] = 0.79*p_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_balance_outstanding_fc'] = 0.79*q_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_balance_outstanding_myr'] = 0.79*r_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_drawdown_myr'] = 0.79*s_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776'),'acc_repayment_myr'] = 0.79*t_humm

    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'facility_amount_outstanding'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_principal_amount_outstanding'] = 0.21*b_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_accrued_interest_month_fc'] = 0.21*c_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_accrued_interest_month_myr'] = 0.21*d_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_modification_loss'] = 0.21*e_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_modification_loss_myr'] = 0.21*f_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_accurate_interest'] = 0.21*g_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_accrued_interest_myr'] = 0.21*h_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_suspended_interest'] = 0.21*i_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_interest_suspense_myr'] = 0.21*j_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_other_charges'] = 0.21*k_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_other_charges_myr'] = 0.21*l_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_penalty'] = 0.21*m_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_penalty_myr'] = 0.21*n_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_penalty_compensation_fc'] = 0.21*o_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_penalty_compensation_myr'] = 0.21*p_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_balance_outstanding_fc'] = 0.21*q_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_balance_outstanding_myr'] = 0.21*r_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_drawdown_myr'] = 0.21*s_humm
    appendfinal3.loc[(appendfinal3['finance_sap_number']=='500776A'),'acc_repayment_myr'] = 0.21*t_humm

    convert_time = str(current_time).replace(":","-")
    appendfinal3['position_as_at'] = reportingDate
    #os.path.join(config.FOLDER_CONFIG["FTP_directory"],documentName)
    appendfinal3.to_excel(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_Debtor_Listing_"+str(convert_time)[:19]+".xlsx"),index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

    #df1 =  config.FOLDER_CONFIG["FTP_directory"]+documentName #"ECL 1024 - MIS v1.xlsx" #documentName
except Exception as e:
    print(f"Process Excel Error: {e}")
    sql_query3 = """INSERT INTO [log_apps_error] (
                    [logerror_desc],
                    [iduser],
                    [dateerror],
                    [page],
                    [user_name]
                )
                VALUES
                    (?,  
                    0,  
                    getdate(),  
                    ?,  
                    ?
                    )
                """
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel Debtor Listing",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Process Excel Debtor Listing'
    WHERE [jobName] = 'Debtor Listing';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Process Excel Debtor Listing Error: {e}")
    sys.exit(f"Process Excel Debtor Listing Error: {str(e)}")
    #sys.exit(1) 

    #==============================================================================================

    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Not Applicable",'PY003','PY003')] #,36961,36961
    download_error = pd.DataFrame(data,columns=columns)
    
    # Assuming 'combine2' is a DataFrame
    column_types1 = []
    for col in download_error.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if download_error[col].dtype == 'object':  # String data type
            column_types1.append(f"{col} VARCHAR(255)")
        elif download_error[col].dtype == 'int64':  # Integer data type
            column_types1.append(f"{col} INT")
        elif download_error[col].dtype == 'float64':  # Float data type
            column_types1.append(f"{col} FLOAT")
        else:
            column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

    create_table_query_result = "CREATE TABLE A_download_error (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_error.iterrows():
        sql_result = "INSERT INTO A_download_error({}) VALUES ({})".format(','.join(download_error.columns), ','.join(['?']*len(download_error.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_error AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);    
    """)
    conn.commit() 
    cursor.execute("drop table A_download_error")
    conn.commit() 

#--------------------------------------------------------connect ngan database-----------------------------------------------------------------------------------------------------------------------------------------------------

# cntrl + K + C untuk comment kn sume 
# cntrl + K + U untuk comment kn sume 

try:
    #table        
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Result_Debtor_Listing_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')]
    download_result = pd.DataFrame(data,columns=columns)
    
    # Assuming 'combine2' is a DataFrame
    column_types1 = []
    for col in download_result.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if download_result[col].dtype == 'object':  # String data type
            column_types1.append(f"{col} VARCHAR(255)")
        elif download_result[col].dtype == 'int64':  # Integer data type
            column_types1.append(f"{col} INT")
        elif download_result[col].dtype == 'float64':  # Float data type
            column_types1.append(f"{col} FLOAT")
        else:
            column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

    create_table_query_result = "CREATE TABLE A_download_result_A (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_result.iterrows():
        sql_result = "INSERT INTO A_download_result_A({}) VALUES ({})".format(','.join(download_result.columns), ','.join(['?']*len(download_result.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()


    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_result_A AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);      
    """)
    conn.commit() 
    cursor.execute("drop table A_download_result_A")
    conn.commit() 

    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # Assuming 'combine2' is a DataFrame
    column_types = []
    for col in appendfinal3.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if appendfinal3[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif appendfinal3[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif appendfinal3[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others

    #cursor.execute("CREATE TABLE A_DEBTOR ({})".format(','.join(appendfinal3.columns)))
    
    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_DEBTOR (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in appendfinal3.iterrows():
        sql = "INSERT INTO A_DEBTOR({}) VALUES ({})".format(','.join(appendfinal3.columns), ','.join(['?']*len(appendfinal3.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()


    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_DEBTOR AS source
    ON target.finance_sap_number = source.finance_sap_number
    WHEN MATCHED THEN
        UPDATE SET target.facility_amount_outstanding = source.facility_amount_outstanding,
                target.acc_principal_amount_outstanding = source.acc_principal_amount_outstanding,
                target.acc_accrued_interest_month_fc = source.acc_accrued_interest_month_fc,
                target.acc_accrued_interest_month_myr = source.acc_accrued_interest_month_myr,
                target.acc_modification_loss = source.acc_modification_loss,
                target.acc_modification_loss_myr = source.acc_modification_loss_myr,
                target.acc_accurate_interest = source.acc_accurate_interest,
                target.acc_accrued_interest_myr = source.acc_accrued_interest_myr,
                target.acc_suspended_interest = source.acc_suspended_interest,
                target.acc_interest_suspense_myr = source.acc_interest_suspense_myr,
                target.acc_other_charges = source.acc_other_charges,
                target.acc_other_charges_myr = source.acc_other_charges_myr,
                target.acc_penalty = source.acc_penalty,
                target.acc_penalty_myr = source.acc_penalty_myr,
                target.acc_penalty_compensation_fc = source.acc_penalty_compensation_fc,
                target.acc_penalty_compensation_myr = source.acc_penalty_compensation_myr,
                target.acc_balance_outstanding_fc = source.acc_balance_outstanding_fc,
                target.acc_balance_outstanding_myr = source.acc_balance_outstanding_myr,
                target.acc_drawdown_myr = source.acc_drawdown_myr,
                target.acc_repayment_myr = source.acc_repayment_myr,
                target.position_as_at = source.position_as_at;
    """)
    conn.commit() 

    cursor.execute("drop table A_DEBTOR")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'Debtor Listing';"""
    cursor.execute(sql_query4)
    conn.commit() 

    print("Data updated successfully at "+str(current_time))
    conn.close()
except Exception as e:
    #print(appendfinal3.dtypes)
    print(f"Update Database Error: {e}")
    sql_query5 = """INSERT INTO [log_apps_error] (
                    [logerror_desc],
                    [iduser],
                    [dateerror],
                    [page],
                    [user_name]
                )
                VALUES
                    (?,  
                    0,  
                    getdate(),  
                    ?,  
                    ?
                    )
                """
    cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database Debtor Listing",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Update Database Debtor Listing'
    WHERE [jobName] = 'Debtor Listing';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Update Database Debtor Listing Error: {e}")
    sys.exit(f"Update Database Debtor Listing Error: {str(e)}")

    #==============================================================================================

    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Not Applicable",'PY003','PY003')] #,36961,36961
    download_error = pd.DataFrame(data,columns=columns)
    
    # Assuming 'combine2' is a DataFrame
    column_types1 = []
    for col in download_error.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if download_error[col].dtype == 'object':  # String data type
            column_types1.append(f"{col} VARCHAR(255)")
        elif download_error[col].dtype == 'int64':  # Integer data type
            column_types1.append(f"{col} INT")
        elif download_error[col].dtype == 'float64':  # Float data type
            column_types1.append(f"{col} FLOAT")
        else:
            column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

    create_table_query_result = "CREATE TABLE A_download_error (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_error.iterrows():
        sql_result = "INSERT INTO A_download_error({}) VALUES ({})".format(','.join(download_error.columns), ','.join(['?']*len(download_error.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_error AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);    
    """)
    conn.commit() 
    cursor.execute("drop table A_download_error")
    conn.commit() 
    
    #sys.exit(1)
#except Exception as e:
#    print(f"Python Error: {e}")
#    sys.exit(f"Python Error: {str(e)}")
#    sys.exit(1)