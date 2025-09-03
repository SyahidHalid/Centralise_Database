# python Data_Mirror.py 12,"Data Mirror October 2024.xlsx","Data Mirror","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python Data_Mirror.py 12 "DataMirrorMay2025.xlsx.xlsx.xlsx" "Data Mirror" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-05-31"
# position_as_at
#aftd_id = DocumentId
#tmbh update result table

#try:
import os
import sys
import pyodbc
import config

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
        sys.exit(1)  # Exit the script with a failure code
    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
            print("Database connection closed.")
        
#----------------------------------------------------------------------------------------------------

try:
    #   Library
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
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Data_Mirror.py',[jobCompleted] = NULL,[jobErrDetail]=?
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_query1,os.getcwd())
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")
    sys.exit(f"Connect to Database Error: {str(e)}")
    #sys.exit(1)
        
#----------------------------------------------------------------------------------------------------

#upload excel
try:
    #   Excel File Name

    #E:mis_doc\\PythonProjects\\misPython\\misPython_doc
    #df1 = documentName #"Data Mirror October 2024.xlsx"
    #import config
    #   documentName = "DataMirrorJuly2025.xlsx.xlsx"
    #   reportingDate = "2025-07-31"

    df1 = os.path.join(config.FOLDER_CONFIG["FTP_directory"],documentName) #"ECL 1024 - MIS v1.xlsx" #documentName

    #   Excel Sheet Name
    D1 = "Interest"
    D2 = "Profit"
    D3 = "Other Charges Conv"
    D4 = "Other Charges Islamic"
    D5 = "IIS"
    D6 = "PIS"
    D7 = "Penalty"
    D8 = "Ta'widh Active"
    D9 = "Ta'widh Recovery"
    #   Upload
    Interest = pd.read_excel(df1, sheet_name=D1, header=8)
    Profit = pd.read_excel(df1, sheet_name=D2, header=8)
    Other_payment_conv = pd.read_excel(df1, sheet_name=D3, header=8)
    Other_payment_isl = pd.read_excel(df1, sheet_name=D4, header=8)
    IIS = pd.read_excel(df1, sheet_name=D5, header=8)
    PIS = pd.read_excel(df1, sheet_name=D6, header=8)
    Penalty = pd.read_excel(df1, sheet_name=D7, header=8)
    T_A = pd.read_excel(df1, sheet_name=D8, header=8)
    T_R = pd.read_excel(df1, sheet_name=D9, header=8)
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
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","Upload Excel Data Mirror",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Upload Excel Data Mirror'
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Upload Excel Data Mirror Error: {e}")
    sys.exit(f"Upload Excel Data Mirror Error: {str(e)}")
    #sys.exit(1) 

    #==============================================================================================

    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Not Applicable",'PY004','PY004')] #,36961,36961
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

#print(current_time.year)
#timestamp = pd.Timestamp('2024-11-21 10:45:00')

# Extract components directly from the Timestamp object
#year = timestamp.year
#month = timestamp.month
#day = timestamp.day
#hour = timestamp.hour
#minute = timestamp.minute
#second = timestamp.second
#print(f"Year: {year}, Month: {month}, Day: {day}, Hour: {hour}, Minute: {minute}, Second: {second}")
#Year: 2024, Month: 11, Day: 21, Hour: 10, Minute: 45, Second: 0

#INSERT INTO Employees (EmployeeID, FirstName, LastName, Position)
#VALUES (1, 'John', 'Doe', 'Manager');

#INSERT INTO Employees
#VALUES (1, 'John', 'Doe', 'Manager');

#   Old DB Method
#LDB_prev = pd.read_excel("Oct 2024_Final Run.xlsx", sheet_name="Loan Database", header=1)

#   SQLALChemy
#connection_string = "mssql+pyodbc://mis_admin:Exim1234@10.32.1.51,1455/mis_db_prod_backup_2024_04_02?driver=ODBC+Driver+17+for+SQL+Server"
#engine = create_engine(connection_string)
#connection = engine.connect()

#LDB_prev = pd.read_sql("SELECT * FROM col_facilities_application_master", con=engine)

#---------------------------------Start---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#process
try:
    Other_payment_conv['Type_of_Financing'] = 'Conventional'
    Other_payment_isl['Type_of_Financing'] = 'Islamic'

    Interest['Type_of_Financing'] = 'Conventional'
    Profit['Type_of_Financing'] = 'Islamic'

    IIS['Type_of_Financing'] = 'Conventional'
    PIS['Type_of_Financing'] = 'Islamic'

    Other_payment_isl.columns = Other_payment_isl.columns.str.replace("\n", "_")
    Other_payment_isl.columns = Other_payment_isl.columns.str.replace(" ", "_")
    Other_payment_isl.columns = Other_payment_isl.columns.str.replace(".", "_")

    Other_payment_conv.columns = Other_payment_conv.columns.str.replace("\n", "_")
    Other_payment_conv.columns = Other_payment_conv.columns.str.replace(" ", "_")
    Other_payment_conv.columns = Other_payment_conv.columns.str.replace(".", "_")

    Profit.columns = Profit.columns.str.replace("\n", "_")
    Profit.columns = Profit.columns.str.replace(" ", "_")
    Profit.columns = Profit.columns.str.replace(".", "_")

    Interest.columns = Interest.columns.str.replace("\n", "_")
    Interest.columns = Interest.columns.str.replace(" ", "_")
    Interest.columns = Interest.columns.str.replace(".", "_")

    IIS.columns = IIS.columns.str.replace("\n", "_")
    IIS.columns = IIS.columns.str.replace(" ", "_")
    IIS.columns = IIS.columns.str.replace(".", "_")

    PIS.columns = PIS.columns.str.replace("\n", "_")
    PIS.columns = PIS.columns.str.replace(" ", "_")
    PIS.columns = PIS.columns.str.replace(".", "_")

    #---------------------------------------------Penalty------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #SJPP
    Other_payment_isl.loc[(~(Other_payment_isl.Account.isna())&(Other_payment_isl.Text.str.contains("SJPP"))),"______Amount_in_DC"] = 0
    Other_payment_isl.loc[(~(Other_payment_isl.Account.isna())&(Other_payment_isl.Text.str.contains("SJPP"))),"___Amt_in_loc_cur_"] = 0

    Other_payment_conv.loc[(~(Other_payment_conv.Account.isna())&(Other_payment_conv.Text.str.contains("SJPP"))),"______Amount_in_DC"] = 0
    Other_payment_conv.loc[(~(Other_payment_conv.Account.isna())&(Other_payment_conv.Text.str.contains("SJPP"))),"___Amt_in_loc_cur_"] = 0

    #Penalty
    IIS.loc[(~(IIS.Account.isna())&(IIS.Text.str.contains("Penalty"))),"Ta`widh Payment/Penalty Repayment (Facility Currency)"] = IIS['______Amount_in_DC']
    IIS.loc[(~(IIS.Account.isna())&(IIS.Text.str.contains("Penalty"))),"Ta`widh Payment/Penalty Repayment (MYR)"] = IIS['___Amt_in_loc_cur_']
    IIS.loc[(~(IIS.Account.isna())&(IIS.Text.str.contains("Penalty"))),"______Amount_in_DC"] = 0
    IIS.loc[(~(IIS.Account.isna())&(IIS.Text.str.contains("Penalty"))),"___Amt_in_loc_cur_"] = 0

    PIS.loc[(~(PIS.Account.isna())&((PIS.Text.str.contains("Penalty"))|(PIS.Text.str.contains("Ta'widh")))),"Ta`widh Payment/Penalty Repayment (Facility Currency)"] = PIS['______Amount_in_DC']
    PIS.loc[(~(PIS.Account.isna())&((PIS.Text.str.contains("Penalty"))|(PIS.Text.str.contains("Ta'widh")))),"Ta`widh Payment/Penalty Repayment (MYR)"] = PIS['___Amt_in_loc_cur_']
    PIS.loc[(~(PIS.Account.isna())&((PIS.Text.str.contains("Penalty"))|(PIS.Text.str.contains("Ta'widh")))),"______Amount_in_DC"] = 0
    PIS.loc[(~(PIS.Account.isna())&((PIS.Text.str.contains("Penalty"))|(PIS.Text.str.contains("Ta'widh")))),"___Amt_in_loc_cur_"] = 0

    #PIS blom ad nme kat text untuk penalty
    
    #---------------------------------------------Process-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    Other_payment_conv1 = Other_payment_conv.iloc[np.where(~(Other_payment_conv.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC']].sum().reset_index()
    Other_payment_conv1['___Amt_in_loc_cur_'] = -1*Other_payment_conv1['___Amt_in_loc_cur_']
    Other_payment_conv1['______Amount_in_DC'] = -1*Other_payment_conv1['______Amount_in_DC']
    Other_payment_conv1['Account'] = Other_payment_conv1['Account'].astype(int)

    Other_payment_isl1 = Other_payment_isl.iloc[np.where(~(Other_payment_isl.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC']].sum().reset_index()
    Other_payment_isl1['___Amt_in_loc_cur_'] = -1*Other_payment_isl1['___Amt_in_loc_cur_']
    Other_payment_isl1['______Amount_in_DC'] = -1*Other_payment_isl1['______Amount_in_DC']
    Other_payment_isl1['Account'] = Other_payment_isl1['Account'].astype(int)

    Profit1 = Profit.iloc[np.where(~(Profit.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC']].sum().reset_index()
    Profit1['___Amt_in_loc_cur_'] = -1*Profit1['___Amt_in_loc_cur_']
    Profit1['______Amount_in_DC'] = -1*Profit1['______Amount_in_DC']
    Profit1['Account'] = Profit1['Account'].astype(int)

    Interest1 = Interest.iloc[np.where(~(Interest.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC']].sum().reset_index()
    Interest1['___Amt_in_loc_cur_'] = -1*Interest1['___Amt_in_loc_cur_']
    Interest1['______Amount_in_DC'] = -1*Interest1['______Amount_in_DC']
    Interest1['Account'] = Interest1['Account'].astype(int)


    IIS1 = IIS.iloc[np.where(~(IIS.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC',
    "Ta`widh Payment/Penalty Repayment (Facility Currency)",
    "Ta`widh Payment/Penalty Repayment (MYR)"]].sum().reset_index()

    IIS1['___Amt_in_loc_cur_'] = -1*IIS1['___Amt_in_loc_cur_']
    IIS1['______Amount_in_DC'] = -1*IIS1['______Amount_in_DC']
    IIS1['Ta`widh Payment/Penalty Repayment (MYR)'] = -1*IIS1['Ta`widh Payment/Penalty Repayment (MYR)']
    IIS1['Ta`widh Payment/Penalty Repayment (Facility Currency)'] = -1*IIS1['Ta`widh Payment/Penalty Repayment (Facility Currency)']
    IIS1['Account'] = IIS1['Account'].astype(int)
    
    PIS1 = PIS.iloc[np.where(~(PIS.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC',
    "Ta`widh Payment/Penalty Repayment (Facility Currency)",
    "Ta`widh Payment/Penalty Repayment (MYR)"]].sum().reset_index()

    PIS1['___Amt_in_loc_cur_'] = -1*PIS1['___Amt_in_loc_cur_']
    PIS1['______Amount_in_DC'] = -1*PIS1['______Amount_in_DC']
    PIS1['Ta`widh Payment/Penalty Repayment (MYR)'] = -1*PIS1['Ta`widh Payment/Penalty Repayment (MYR)']
    PIS1['Ta`widh Payment/Penalty Repayment (Facility Currency)'] = -1*PIS1['Ta`widh Payment/Penalty Repayment (Facility Currency)']
    PIS1['Account'] = PIS1['Account'].astype(int)

    Interest1['Ta`widh Payment/Penalty Repayment (MYR)'] = 0
    Interest1['Ta`widh Payment/Penalty Repayment (Facility Currency)'] = 0
    Profit1['Ta`widh Payment/Penalty Repayment (MYR)'] = 0
    Profit1['Ta`widh Payment/Penalty Repayment (Facility Currency)'] = 0

    merge = pd.concat([Other_payment_conv1,Other_payment_isl1]).fillna(0).rename(columns={'___Amt_in_loc_cur_':'Other_Charges_Payment_MYR','______Amount_in_DC':'Other_Charges_Payment_FC'})
    
    # merge.head(1)
    # sum(merge.Other_Charges_Payment_MYR)

    merge1 = pd.concat([Interest1,IIS1,Profit1,PIS1]).fillna(0).rename(columns={'___Amt_in_loc_cur_':'Profit_Payment_Interest_Repayment_MYR','______Amount_in_DC':'Profit_Payment_Interest_Repayment_FC'})

    #   sum(merge1['Ta`widh Payment/Penalty Repayment (Facility Currency)'])
    #   sum(merge1['Ta`widh Payment/Penalty Repayment (MYR)'])
    
    merge['Account'] = merge['Account'].astype(str)
    merge1['Account'] = merge1['Account'].astype(str)

    #------------------------------------------------------------Cumulative----------------------------------------------------------------------------------------------------------------------------------------------

    LDB_prev['finance_sap_number'] = LDB_prev['finance_sap_number'].astype(str)

    LDB_prev.columns = LDB_prev.columns.str.replace("\n", "")

    LDB_prev['acc_interest_repayment_fc'].fillna(0,inplace=True)
    LDB_prev['acc_interest_repayment_myr'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_interest_repayment_fc'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_interest_repayment_myr'].fillna(0,inplace=True)

    LDB_prev['acc_others_charges_payment_fc'].fillna(0,inplace=True)
    LDB_prev['acc_others_charges_payment_myr'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_others_charge_payment_fc'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_others_charge_payment_myr'].fillna(0,inplace=True)
    LDB_prev['acc_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    LDB_prev['acc_tawidh_payment_repayment_myr'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_tawidh_payment_repayment_myr'].fillna(0,inplace=True)

    #LDB_prev['Cumulative Other Charges Payment (Facility Currency)'].fillna(0,inplace=True)
    #LDB_prev['Cumulative Other Charges Payment (MYR)'].fillna(0,inplace=True)

    #sbb other payment blom ad lg kat MIS
    merge_ldb = merge.merge(LDB_prev.iloc[np.where(LDB_prev['finance_sap_number']!="nan")][['finance_sap_number',
                                    'acc_others_charges_payment_fc','acc_others_charges_payment_myr',
                                    'acc_cumulative_others_charge_payment_fc','acc_cumulative_others_charge_payment_myr',]].drop_duplicates('finance_sap_number',keep='first').rename(columns={'finance_sap_number':'Account'}),on=['Account'],how='left',suffixes=('_x', ''),indicator=True)

    # merge_ldb.head(1)
    # sum(merge_ldb.acc_others_charges_payment_myr)

    merge1_ldb = merge1.merge(LDB_prev.iloc[np.where(LDB_prev['finance_sap_number']!="nan")][['finance_sap_number',
                                    'acc_interest_repayment_fc','acc_interest_repayment_myr',
                                    'acc_cumulative_interest_repayment_fc','acc_cumulative_interest_repayment_myr',
                                    'acc_tawidh_payment_repayment_fc','acc_tawidh_payment_repayment_myr',
                                    'acc_cumulative_tawidh_payment_repayment_fc','acc_cumulative_tawidh_payment_repayment_myr']].drop_duplicates('finance_sap_number',keep='first').rename(columns={'finance_sap_number':'Account'}),on=['Account'],how='left', suffixes=('_x', ''),indicator=True)


    #merge1_ldb.head(1)

    merge_ldb['Other_Charges_Payment_MYR'].fillna(0,inplace=True)
    merge_ldb['Other_Charges_Payment_FC'].fillna(0,inplace=True)
    merge_ldb['acc_others_charges_payment_fc'].fillna(0,inplace=True)
    merge_ldb['acc_others_charges_payment_myr'].fillna(0,inplace=True)
    merge_ldb['acc_cumulative_others_charge_payment_fc'].fillna(0,inplace=True)
    merge_ldb['acc_cumulative_others_charge_payment_myr'].fillna(0,inplace=True)

    #merge_ldb['Cumulative Other Charges Payment (Facility Currency)'].fillna(0,inplace=True) 
    #merge_ldb['Cumulative Other Charges Payment (MYR)'].fillna(0,inplace=True)

    merge1_ldb['Profit_Payment_Interest_Repayment_MYR'].fillna(0,inplace=True)
    merge1_ldb['Profit_Payment_Interest_Repayment_FC'].fillna(0,inplace=True)
    merge1_ldb['Ta`widh Payment/Penalty Repayment (MYR)'].fillna(0,inplace=True) 
    merge1_ldb['Ta`widh Payment/Penalty Repayment (Facility Currency)'].fillna(0,inplace=True)
    merge1_ldb['acc_interest_repayment_fc'].fillna(0,inplace=True)
    merge1_ldb['acc_interest_repayment_myr'].fillna(0,inplace=True)
    merge1_ldb['acc_cumulative_interest_repayment_fc'].fillna(0,inplace=True)
    merge1_ldb['acc_cumulative_interest_repayment_myr'].fillna(0,inplace=True)
    merge1_ldb['acc_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    merge1_ldb['acc_tawidh_payment_repayment_myr'].fillna(0,inplace=True)
    merge1_ldb['acc_cumulative_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    merge1_ldb['acc_cumulative_tawidh_payment_repayment_myr'].fillna(0,inplace=True)

    #merge_ldb['Cumulative Other Charges Payment (MYR) New'] = merge_ldb['Other_Charges_Payment_MYR'] +  merge_ldb['Cumulative Other Charges Payment (MYR)'] 
    #merge_ldb['Cumulative Other Charges Payment (Facility Currency) New'] = merge_ldb['Other_Charges_Payment_FC'] +  merge_ldb['Cumulative Other Charges Payment (Facility Currency)'] 


    merge_ldb['Cumulative_Other_Charges_Payment_FC'] = merge_ldb['Other_Charges_Payment_FC'] + merge_ldb['acc_cumulative_others_charge_payment_fc'] 
    merge_ldb['Cumulative_Other_Charges_Payment_MYR'] = merge_ldb['Other_Charges_Payment_MYR'] + merge_ldb['acc_cumulative_others_charge_payment_myr'] 

    merge1_ldb['Cumulative_Profit_Payment_FC'] = merge1_ldb['acc_cumulative_interest_repayment_fc'] +  merge1_ldb['Profit_Payment_Interest_Repayment_FC'] 
    merge1_ldb['Cumulative_Profit_Payment_MYR'] = merge1_ldb['acc_cumulative_interest_repayment_myr'] + merge1_ldb['Profit_Payment_Interest_Repayment_MYR'] 
    merge1_ldb['Cumulative_Tawidh_Payment_FC'] = merge1_ldb['acc_cumulative_tawidh_payment_repayment_fc'] +  merge1_ldb['Ta`widh Payment/Penalty Repayment (Facility Currency)'] 
    merge1_ldb['Cumulative_Tawidh_Payment_MYR'] = merge1_ldb['acc_cumulative_tawidh_payment_repayment_myr'] +  merge1_ldb['Ta`widh Payment/Penalty Repayment (MYR)'] 


    #to update acc_others_charges_payment_fc, acc_others_charges_payment_myr, acc_cumulative_others_charge_payment_fc, acc_cumulative_others_charge_payment_myr
    merge_ldb = merge_ldb[['Account', 'Other_Charges_Payment_MYR','Other_Charges_Payment_FC',
                        'Cumulative_Other_Charges_Payment_FC','Cumulative_Other_Charges_Payment_MYR']].rename(columns={'Other_Charges_Payment_FC':'acc_others_charges_payment_fc',
                                                                                                                        'Other_Charges_Payment_MYR':'acc_others_charges_payment_myr',
                                                                                                                        'Cumulative_Other_Charges_Payment_FC':'acc_cumulative_others_charge_payment_fc',
                                                                                                                        'Cumulative_Other_Charges_Payment_MYR':'acc_cumulative_others_charge_payment_myr'})
    # sum(merge_ldb.acc_others_charges_payment_myr)

    #to update acc_interest_repayment_fc, acc_interest_repayment_myr, acc_cumulative_interest_repayment_fc, acc_cumulative_interest_repayment_myr,
    #to update acc_tawidh_payment_repayment_fc, acc_tawidh_payment_repayment_myr, acc_cumulative_tawidh_payment_repayment_fc, acc_cumulative_tawidh_payment_repayment_myr
    merge1_ldb = merge1_ldb[['Account', 'Profit_Payment_Interest_Repayment_MYR','Profit_Payment_Interest_Repayment_FC',
                            "Ta`widh Payment/Penalty Repayment (MYR)","Ta`widh Payment/Penalty Repayment (Facility Currency)",
                            'Cumulative_Profit_Payment_FC','Cumulative_Profit_Payment_MYR',
                            'Cumulative_Tawidh_Payment_FC','Cumulative_Tawidh_Payment_MYR']].rename(columns={'Profit_Payment_Interest_Repayment_FC':'acc_interest_repayment_fc',
                                                                                    'Profit_Payment_Interest_Repayment_MYR':'acc_interest_repayment_myr',
                                                                                    'Cumulative_Profit_Payment_FC':'acc_cumulative_interest_repayment_fc',
                                                                                    'Cumulative_Profit_Payment_MYR':'acc_cumulative_interest_repayment_myr',
                                                                                    'Ta`widh Payment/Penalty Repayment (Facility Currency)':'acc_tawidh_payment_repayment_fc',
                                                                                    "Ta`widh Payment/Penalty Repayment (MYR)":'acc_tawidh_payment_repayment_myr',
                                                                                    'Cumulative_Tawidh_Payment_FC':'acc_cumulative_tawidh_payment_repayment_fc',
                                                                                    'Cumulative_Tawidh_Payment_MYR':'acc_cumulative_tawidh_payment_repayment_myr'})


    #combine
    combine = merge_ldb.merge(merge1_ldb,on="Account", how="outer").fillna(0) #,indicator=True
    # combine.iloc[np.where(combine.Account=='500538')]
    # sum(combine.acc_others_charges_payment_myr)
    # sum(combine.acc_tawidh_payment_repayment_fc)
    # sum(combine.acc_tawidh_payment_repayment_myr)

    #---------------------------------------------Ta'widh--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    Penalty.columns = Penalty.columns.str.replace("\n", "_")
    Penalty.columns = Penalty.columns.str.replace(" ", "_")
    Penalty.columns = Penalty.columns.str.replace(".", "_")

    T_A.columns = T_A.columns.str.replace("\n", "_")
    T_A.columns = T_A.columns.str.replace(" ", "_")
    T_A.columns = T_A.columns.str.replace(".", "_")

    T_R.columns = T_R.columns.str.replace("\n", "_")
    T_R.columns = T_R.columns.str.replace(" ", "_")
    T_R.columns = T_R.columns.str.replace(".", "_")

    Penalty.columns = T_A.columns = T_R.columns

    Penalty['Type_of_Financing'] = 'Conventional'
    T_A['Type_of_Financing'] = 'Islamic'
    T_R['Type_of_Financing'] = 'Islamic'

    Penalty1 = Penalty.iloc[np.where(~(Penalty.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC']].sum().reset_index()
    T_A1 = T_A.iloc[np.where(~(T_A.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC']].sum().reset_index()
    T_R1 = T_R.iloc[np.where(~(T_R.Account.isna()))].fillna(0).groupby(['Account','Type_of_Financing'])[['___Amt_in_loc_cur_','______Amount_in_DC']].sum().reset_index()

    Tawidh_Comb = pd.concat([Penalty1, T_A1, T_R1])

    Tawidh_Comb['___Amt_in_loc_cur_'] = -1*Tawidh_Comb['___Amt_in_loc_cur_']
    Tawidh_Comb['______Amount_in_DC'] = -1*Tawidh_Comb['______Amount_in_DC']

    Tawidh_Comb.rename(columns={'___Amt_in_loc_cur_':"Tawidh_Payment_Penalty_Repayment_MYR",
        '______Amount_in_DC':"Tawidh_Payment_Penalty_Repayment_FC"},inplace=True)

    Tawidh_Comb['Account'] = Tawidh_Comb['Account'].astype(int)
    Tawidh_Comb['Account'] = Tawidh_Comb['Account'].astype(str)


    Tawidh_Comb1 = Tawidh_Comb.merge(LDB_prev.iloc[np.where(LDB_prev['finance_sap_number']!="nan")][['finance_sap_number',
                                    'acc_tawidh_payment_repayment_fc','acc_tawidh_payment_repayment_myr',
                                    'acc_cumulative_tawidh_payment_repayment_fc','acc_cumulative_tawidh_payment_repayment_myr']].drop_duplicates('finance_sap_number',keep='first').rename(columns={'finance_sap_number':'Account'}),on=['Account'],how='left', suffixes=('_x', ''),indicator=True)

    Tawidh_Comb1['Tawidh_Payment_Penalty_Repayment_MYR'].fillna(0,inplace=True) 
    Tawidh_Comb1['Tawidh_Payment_Penalty_Repayment_FC'].fillna(0,inplace=True)

    Tawidh_Comb1['acc_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    Tawidh_Comb1['acc_tawidh_payment_repayment_myr'].fillna(0,inplace=True)
    Tawidh_Comb1['acc_cumulative_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    Tawidh_Comb1['acc_cumulative_tawidh_payment_repayment_myr'].fillna(0,inplace=True)

    Tawidh_Comb1['Cumulative_Tawidh_Payment_Penalty_Repayment_FC'] = Tawidh_Comb1['Tawidh_Payment_Penalty_Repayment_FC']  +  Tawidh_Comb1['acc_cumulative_tawidh_payment_repayment_fc'] 
    Tawidh_Comb1['Cumulative_Tawidh_Payment_Penalty_Repayment_MYR'] = Tawidh_Comb1['Tawidh_Payment_Penalty_Repayment_MYR'] +  Tawidh_Comb1['acc_cumulative_tawidh_payment_repayment_myr'] 

    #to update acc_tawidh_payment_repayment_fc, acc_tawidh_payment_repayment_myr, acc_cumulative_tawidh_payment_repayment_fc, acc_cumulative_tawidh_payment_repayment_myr
    Tawidh_Comb1 = Tawidh_Comb1[['Account',
    'Tawidh_Payment_Penalty_Repayment_FC',
    'Tawidh_Payment_Penalty_Repayment_MYR',
    'Cumulative_Tawidh_Payment_Penalty_Repayment_FC',
    'Cumulative_Tawidh_Payment_Penalty_Repayment_MYR']].rename(columns={'Tawidh_Payment_Penalty_Repayment_FC':'acc_tawidh_payment_repayment_fc',
                    'Tawidh_Payment_Penalty_Repayment_MYR':'acc_tawidh_payment_repayment_myr',
                    'Cumulative_Tawidh_Payment_Penalty_Repayment_FC':'acc_cumulative_tawidh_payment_repayment_fc',
                    'Cumulative_Tawidh_Payment_Penalty_Repayment_MYR':'acc_cumulative_tawidh_payment_repayment_myr'})

    # .drop(['acc_tawidh_payment_repayment_fc',
    #                             'acc_tawidh_payment_repayment_myr',
    #                             'acc_cumulative_tawidh_payment_repayment_fc',
    #                             'acc_cumulative_tawidh_payment_repayment_myr'],axis=1)

    #combine2
    combine2 = combine.merge(Tawidh_Comb1,on="Account", how="outer", suffixes=('_Sap','_Mis')).fillna(0) #,indicator=True
    # combine2.iloc[np.where(combine2.Account=='500538')]
    # sum(combine2.acc_others_charges_payment_myr)
    
    combine2["acc_tawidh_payment_repayment_fc"] = combine2["acc_tawidh_payment_repayment_fc_Sap"].fillna(0) + combine2["acc_tawidh_payment_repayment_fc_Mis"].fillna(0)
    combine2["acc_tawidh_payment_repayment_myr"] = combine2["acc_tawidh_payment_repayment_myr_Sap"].fillna(0) + combine2["acc_tawidh_payment_repayment_myr_Mis"].fillna(0)
    combine2["acc_cumulative_tawidh_payment_repayment_fc"] = combine2["acc_cumulative_tawidh_payment_repayment_fc_Sap"].fillna(0) + combine2["acc_cumulative_tawidh_payment_repayment_fc_Mis"].fillna(0)
    combine2["acc_cumulative_tawidh_payment_repayment_myr"] = combine2["acc_cumulative_tawidh_payment_repayment_myr_Sap"].fillna(0) + combine2["acc_cumulative_tawidh_payment_repayment_myr_Mis"].fillna(0)

    combine2.drop(['acc_tawidh_payment_repayment_fc_Sap','acc_tawidh_payment_repayment_fc_Mis',
                                'acc_tawidh_payment_repayment_myr_Sap','acc_tawidh_payment_repayment_myr_Mis',
                                'acc_cumulative_tawidh_payment_repayment_fc_Sap','acc_cumulative_tawidh_payment_repayment_fc_Mis',
                                'acc_cumulative_tawidh_payment_repayment_myr_Sap','acc_cumulative_tawidh_payment_repayment_myr_Mis'],axis=1,inplace=True)
      
    
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
                        0]], columns=['Account',
                                              'acc_others_charges_payment_myr',
                                              'acc_others_charges_payment_fc',
                                              'acc_cumulative_others_charge_payment_fc',
                                              'acc_cumulative_others_charge_payment_myr',
                                              'acc_interest_repayment_myr',
                                              'acc_interest_repayment_fc',
                                              'acc_cumulative_interest_repayment_fc',
                                              'acc_cumulative_interest_repayment_myr',
                                              'acc_tawidh_payment_repayment_fc',
                                              'acc_tawidh_payment_repayment_myr',
                                              'acc_cumulative_tawidh_payment_repayment_fc',
                                              'acc_cumulative_tawidh_payment_repayment_myr'])

    combine2 = pd.concat([combine2, df_add_Humm])

    a_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_interest_repayment_fc'])
    b_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_interest_repayment_myr'])
    c_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_cumulative_interest_repayment_fc'])
    d_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_cumulative_interest_repayment_myr'])
    e_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_others_charges_payment_fc'])
    f_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_others_charges_payment_myr'])
    g_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_cumulative_others_charge_payment_fc'])
    h_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_cumulative_others_charge_payment_myr'])
    i_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_tawidh_payment_repayment_fc'])
    j_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_tawidh_payment_repayment_myr'])
    k_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_cumulative_tawidh_payment_repayment_fc'])
    l_humm = sum(combine2.fillna(0).iloc[np.where(combine2['Account']=='500776')]['acc_cumulative_tawidh_payment_repayment_myr'])

    combine2.loc[(combine2['Account']=='500776'),'acc_interest_repayment_fc'] = 0.79*a_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_interest_repayment_myr'] = 0.79*b_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_cumulative_interest_repayment_fc'] = 0.79*c_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_cumulative_interest_repayment_myr'] = 0.79*d_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_others_charges_payment_fc'] = 0.79*e_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_others_charges_payment_myr'] = 0.79*f_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_cumulative_others_charge_payment_fc'] = 0.79*g_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_cumulative_others_charge_payment_myr'] = 0.79*h_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_tawidh_payment_repayment_fc'] = 0.79*i_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_tawidh_payment_repayment_myr'] = 0.79*j_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_cumulative_tawidh_payment_repayment_fc'] = 0.79*k_humm
    combine2.loc[(combine2['Account']=='500776'),'acc_cumulative_tawidh_payment_repayment_myr'] = 0.79*l_humm

    combine2.loc[(combine2['Account']=='500776A'),'acc_interest_repayment_fc'] = 0.21*a_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_interest_repayment_myr'] = 0.21*b_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_cumulative_interest_repayment_fc'] = 0.21*c_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_cumulative_interest_repayment_myr'] = 0.21*d_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_others_charges_payment_fc'] = 0.21*e_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_others_charges_payment_myr'] = 0.21*f_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_cumulative_others_charge_payment_fc'] = 0.21*g_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_cumulative_others_charge_payment_myr'] = 0.21*h_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_tawidh_payment_repayment_fc'] = 0.21*i_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_tawidh_payment_repayment_myr'] = 0.21*j_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_cumulative_tawidh_payment_repayment_fc'] = 0.21*k_humm
    combine2.loc[(combine2['Account']=='500776A'),'acc_cumulative_tawidh_payment_repayment_myr'] = 0.21*l_humm

    convert_time = str(current_time).replace(":","-")
    combine2['position_as_at'] = reportingDate


    # sum(combine2.acc_others_charges_payment_myr)
    #---------------------------------------------------------cumulative---------------------------------------------------------------------
    LDB_cum = pd.read_sql_query("SELECT * FROM dbase_account_hist;", conn)
    
    reportingYear = int(reportingDate[:4])

    # Convert to datetime
    LDB_cum['position_as_at'] = pd.to_datetime(LDB_cum['position_as_at'], errors='coerce')

    # Filter by year
    LDB_cum_filtered = LDB_cum.loc[(LDB_cum['position_as_at'].dt.year == reportingYear)&(LDB_cum['position_as_at']<reportingDate)]

    LDB_cum_filtered['other_charges_payment_myr'] = LDB_cum_filtered['other_charges_payment_myr'].astype(float)
    LDB_cum_filtered['other_charges_payment'] = LDB_cum_filtered['other_charges_payment'].astype(float)
    LDB_cum_filtered['acc_interest_repayment_myr'] = LDB_cum_filtered['acc_interest_repayment_myr'].astype(float)
    LDB_cum_filtered['acc_interest_repayment_fc'] = LDB_cum_filtered['acc_interest_repayment_fc'].astype(float)
    LDB_cum_filtered['penalty_repayment'] = LDB_cum_filtered['penalty_repayment'].astype(float)
    LDB_cum_filtered['penalty_repayment_myr'] = LDB_cum_filtered['penalty_repayment_myr'].astype(float)
    
    LDB_cum_group = LDB_cum_filtered.iloc[np.where(~(LDB_cum_filtered.finance_sap_number.isna())&
                                                   (LDB_cum_filtered.finance_sap_number!='')&
                                                   (LDB_cum_filtered.finance_sap_number!='NEW ACCOUNT'))].groupby(['finance_sap_number'])[['other_charges_payment_myr',
                                                                      'other_charges_payment',
                                                                      'acc_interest_repayment_myr',
                                                                      'acc_interest_repayment_fc',
                                                                      'penalty_repayment',
                                                                      'penalty_repayment_myr']].sum().reset_index()
    
    LDB_cum_group.rename(columns={'other_charges_payment_myr':'acc_cumulative_others_charge_payment_myr',
                                  'other_charges_payment':'acc_cumulative_others_charge_payment_fc',
                                  'acc_interest_repayment_myr':'acc_cumulative_interest_repayment_myr',
                                  'acc_interest_repayment_fc':'acc_cumulative_interest_repayment_fc',
                                  'penalty_repayment_myr':'acc_cumulative_tawidh_payment_repayment_myr',
                                  'penalty_repayment':'acc_cumulative_tawidh_payment_repayment_fc'},inplace=True)

    combine3 = combine2[['Account',
                                             'acc_others_charges_payment_fc',
                                             'acc_others_charges_payment_myr',
                                             'acc_interest_repayment_fc',
                                             'acc_interest_repayment_myr',
                                             'acc_tawidh_payment_repayment_fc',
                                             'acc_tawidh_payment_repayment_myr',
                                             'position_as_at']].rename(columns={'Account':'finance_sap_number'}).merge(LDB_cum_group,
                                             on='finance_sap_number',
                                             how='outer',
                                             indicator=True)
    combine3['position_as_at'] = reportingDate
    #combine3['cif_name'].fillna('Only Exist in FAD',inplace=True)
    combine3['acc_others_charges_payment_fc'].fillna(0,inplace=True)
    combine3['acc_cumulative_others_charge_payment_fc'].fillna(0,inplace=True)
    combine3['acc_others_charges_payment_myr'].fillna(0,inplace=True)
    combine3['acc_cumulative_others_charge_payment_myr'].fillna(0,inplace=True)
    combine3['acc_interest_repayment_fc'].fillna(0,inplace=True)
    combine3['acc_cumulative_interest_repayment_fc'].fillna(0,inplace=True)
    combine3['acc_interest_repayment_myr'].fillna(0,inplace=True)
    combine3['acc_cumulative_interest_repayment_myr'].fillna(0,inplace=True)
    combine3['acc_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    combine3['acc_cumulative_tawidh_payment_repayment_fc'].fillna(0,inplace=True)
    combine3['acc_tawidh_payment_repayment_myr'].fillna(0,inplace=True)
    combine3['acc_cumulative_tawidh_payment_repayment_myr'].fillna(0,inplace=True)
        
    # LDB_cum_group.finance_sap_number.value_counts()
    # LDB_cum_group.iloc[np.where(LDB_cum_group.finance_sap_number=='501166')]
    # LDB_cum_group.head(1)
    
    # combine3.head(1)
    # sum(combine3.acc_others_charges_payment_myr)
    # combine3.iloc[np.where(combine3.acc_others_charges_payment_myr>0)]

    combine3['acc_cumulative_others_charge_payment_fc'] = combine3['acc_others_charges_payment_fc'].fillna(0) + combine3['acc_cumulative_others_charge_payment_fc'].fillna(0)
    combine3['acc_cumulative_others_charge_payment_myr'] = combine3['acc_others_charges_payment_myr'].fillna(0) + combine3['acc_cumulative_others_charge_payment_myr'].fillna(0)
    combine3['acc_cumulative_interest_repayment_fc'] = combine3['acc_interest_repayment_fc'].fillna(0) + combine3['acc_cumulative_interest_repayment_fc'].fillna(0)
    combine3['acc_cumulative_interest_repayment_myr'] = combine3['acc_interest_repayment_myr'].fillna(0) + combine3['acc_cumulative_interest_repayment_myr'].fillna(0)
    combine3['acc_cumulative_tawidh_payment_repayment_fc'] = combine3['acc_tawidh_payment_repayment_fc'].fillna(0) + combine3['acc_cumulative_tawidh_payment_repayment_fc'].fillna(0)
    combine3['acc_cumulative_tawidh_payment_repayment_myr'] = combine3['acc_tawidh_payment_repayment_myr'].fillna(0) + combine3['acc_cumulative_tawidh_payment_repayment_myr'].fillna(0)
    
    combine3 = combine3[['finance_sap_number',#'cif_name',
                         '_merge',
                         'acc_others_charges_payment_fc','acc_others_charges_payment_myr','acc_cumulative_others_charge_payment_fc','acc_cumulative_others_charge_payment_myr',
                         'acc_interest_repayment_fc','acc_interest_repayment_myr','acc_cumulative_interest_repayment_fc','acc_cumulative_interest_repayment_myr',
                         'acc_tawidh_payment_repayment_fc','acc_tawidh_payment_repayment_myr','acc_cumulative_tawidh_payment_repayment_fc','acc_cumulative_tawidh_payment_repayment_myr',
                         'position_as_at']].drop_duplicates('finance_sap_number',keep='first')#.fillna(0)#.sort_values(by='_merge',ascending=True)


    # combine2.shape
    # combine3._merge.value_counts()
    # combine3.head(1)
    # LDB_cum_filtered.position_as_at.value_counts()
    # LDB_cum_group.finance_sap_number.value_counts()

    #---------------------------------------------exception--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    LDB_name = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = ?;", conn, params=(reportingDate,))

    LDB_hist_before = pd.read_sql_query("SELECT * FROM col_facilities_application_master where position_as_at = ?;", conn, params=(reportingDate,))
   
    LDB_hist = LDB_hist_before.merge(LDB_name[['finance_sap_number','cif_name']], on='finance_sap_number', how='left')
    
    LDB_hist.acc_tawidh_payment_repayment_fc = LDB_hist.acc_tawidh_payment_repayment_fc.astype(float)
    LDB_hist.acc_others_charges_payment_fc = LDB_hist.acc_others_charges_payment_fc.astype(float)
    LDB_hist.acc_interest_repayment_fc = LDB_hist.acc_interest_repayment_fc.astype(float)

    condition1 = ~LDB_hist.finance_sap_number.isna()
    #condition2 = (LDB_hist.penalty_repayment > 0) | (LDB_hist.other_charges_payment > 0) | (LDB_hist.acc_interest_repayment_fc > 0)

    # LDB_hist.head(1)
    LDB_hist1 = LDB_hist[['finance_sap_number',
                                                                  'cif_name',
                                                   'acc_tawidh_payment_repayment_fc',
                                                   'acc_tawidh_payment_repayment_myr',
                                                   'acc_others_charges_payment_fc',
                                                   'acc_others_charges_payment_myr',
                                                   'acc_interest_repayment_fc',
                                                   'acc_interest_repayment_myr']] #.iloc[np.where(condition1 & condition2)]
    # combine2.head(1)
    # combine2.shape

    exception_report = combine2.rename(columns={'Account':'finance_sap_number'}).merge(LDB_hist1, on='finance_sap_number', how='outer', suffixes=('_Sap','_Mis'),indicator=True)

    # exception_report.head(1)

    #Ta`widh Payment/Penalty Repayment  (Facility Currency)
    exception_report["diff_tawidh_payment_fc"] = exception_report["acc_tawidh_payment_repayment_fc_Sap"].fillna(0) - exception_report["acc_tawidh_payment_repayment_fc_Mis"].fillna(0)
    
    #Ta`widh Payment/Penalty Repayment  (MYR)
    exception_report["diff_tawidh_payment_myr"] = exception_report["acc_tawidh_payment_repayment_myr_Sap"].fillna(0) - exception_report["acc_tawidh_payment_repayment_myr_Mis"].fillna(0)
    
    #Other Charges Payment (Facility Currency)
    exception_report["diff_others_charges_payment_fc"] = exception_report["acc_others_charges_payment_fc_Sap"].fillna(0) - exception_report["acc_others_charges_payment_fc_Mis"].fillna(0)
    
    #Other Charges Payment  (MYR)
    exception_report["diff_others_charges_payment_myr"] = exception_report["acc_others_charges_payment_myr_Sap"].fillna(0) - exception_report["acc_others_charges_payment_myr_Mis"].fillna(0)

    #Profit Payment/Interest Repayment (Facility Currency)
    exception_report["diff_profit_payment_fc"] = exception_report["acc_interest_repayment_fc_Sap"].fillna(0) - exception_report["acc_interest_repayment_fc_Mis"].fillna(0)
    
    #Profit Payment/Interest Repayment (MYR)
    exception_report["diff_profit_payment_myr"] = exception_report["acc_interest_repayment_myr_Sap"].fillna(0) - exception_report["acc_interest_repayment_myr_Mis"].fillna(0)

    exception_report.position_as_at.fillna(reportingDate,inplace=True)

    exception_report1 = exception_report[['finance_sap_number',
                                          'cif_name',
                                          'position_as_at',
                                          '_merge',
                                          'acc_tawidh_payment_repayment_fc_Sap',
                                          'acc_tawidh_payment_repayment_fc_Mis',
                                          'diff_tawidh_payment_fc',
                                          'acc_tawidh_payment_repayment_myr_Sap',
                                          'acc_tawidh_payment_repayment_myr_Mis',
                                          'diff_tawidh_payment_myr',
                                          'acc_others_charges_payment_fc_Sap',
                                          'acc_others_charges_payment_fc_Mis',
                                          'diff_others_charges_payment_fc',
                                          'acc_others_charges_payment_myr_Sap',
                                          'acc_others_charges_payment_myr_Mis',
                                          'diff_others_charges_payment_myr',
                                          'acc_interest_repayment_fc_Sap',
                                          'acc_interest_repayment_fc_Mis',
                                          'diff_profit_payment_fc',
                                          'acc_interest_repayment_myr_Sap',
                                          'acc_interest_repayment_myr_Mis',
                                          'diff_profit_payment_myr']]

    # combine3.head(1)
    # sum(combine3.acc_others_charges_payment_fc)
    # sum(combine3.acc_others_charges_payment_myr)
    # sum(combine3.acc_tawidh_payment_repayment_fc)
    # sum(combine3.acc_tawidh_payment_repayment_myr)

    # Extract
    writer2 = pd.ExcelWriter(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_Data_Mirror_"+str(convert_time)[:19]+".xlsx"),engine='xlsxwriter')

    combine3.to_excel(writer2, sheet_name='Result', index = False)

    exception_report1.to_excel(writer2, sheet_name='Exception', index = False)

    writer2.close()

    # combine2.to_excel(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_Data_Mirror_"+str(convert_time)[:19]+".xlsx"),index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

    #combine2.to_excel("Data_Mirror_"+str(convert_time)[:19]+".xlsx",index=False)
    #df1 =  config.FOLDER_CONFIG["FTP_directory"]+documentName #"ECL 1024 - MIS v1.xlsx" #documentName
    
    #combine2.dtypes
    #combine2.shape
    #combine2.to_excel('05. Profit Payment & Other Charges Payment.xlsx', index=False)

    cursor.execute("DROP TABLE IF EXISTS Exception_Data_Mirror")
    conn.commit()

    exception_report1._merge = exception_report1._merge.astype(str)
    exception_report1.fillna(0,inplace=True)
    
    # Assuming 'combine2' is a DataFrame
    column_types1 = []
    for col in exception_report1.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if exception_report1[col].dtype == 'object':  # String data type
            column_types1.append(f"{col} VARCHAR(255)")
        elif exception_report1[col].dtype == 'int64':  # Integer data type
            column_types1.append(f"{col} INT")
        elif exception_report1[col].dtype == 'float64':  # Float data type
            column_types1.append(f"{col} FLOAT")
        else:
            column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

    #   exception_report1.dtypes
    create_table_query_result = "CREATE TABLE Exception_Data_Mirror (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in exception_report1.iterrows():
        sql_result = "INSERT INTO Exception_Data_Mirror({}) VALUES ({})".format(','.join(exception_report1.columns), ','.join(['?']*len(exception_report1.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()

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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel Data Mirror",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Process Excel Data Mirror'
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Process Excel Data Mirror Error: {e}")
    sys.exit(f"Process Excel Data Mirror Error: {str(e)}")
    #sys.exit(1) 

    #==============================================================================================

    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Not Applicable",'PY004','PY004')] #,36961,36961
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

#smpi sini
#current_time = pd.Timestamp.now()

# cntrl + K + C untuk comment kn sume 
# cntrl + K + U untuk comment kn sume 

try:


    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # Assuming 'combine2' is a DataFrame
    column_types = []
    for col in combine3.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if combine3[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif combine3[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif combine3[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others


    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_PROFIT_N_OTHER_PAYMENT (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in combine3.iterrows():
        sql = "INSERT INTO A_PROFIT_N_OTHER_PAYMENT({}) VALUES ({})".format(','.join(combine3.columns), ','.join(['?']*len(combine3.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()

    cursor.execute("""
    WITH DistinctSource AS (
        SELECT 
            finance_sap_number,
            acc_interest_repayment_myr,
            acc_cumulative_interest_repayment_myr,
            acc_tawidh_payment_repayment_fc,
            acc_tawidh_payment_repayment_myr,
            acc_cumulative_tawidh_payment_repayment_fc,
            acc_cumulative_tawidh_payment_repayment_myr,
            acc_others_charges_payment_fc,
            acc_others_charges_payment_myr,
            acc_cumulative_others_charge_payment_fc,
            acc_cumulative_others_charge_payment_myr,
            ROW_NUMBER() OVER (PARTITION BY finance_sap_number ORDER BY finance_sap_number) AS rn
        FROM A_PROFIT_N_OTHER_PAYMENT
    )
    MERGE INTO col_facilities_application_master AS target
    USING (SELECT * FROM DistinctSource WHERE rn = 1) AS source
    ON target.finance_sap_number = source.finance_sap_number
    WHEN MATCHED AND target.position_as_at = ? THEN
        UPDATE SET 
            target.acc_interest_repayment_myr = source.acc_interest_repayment_myr
            target.acc_cumulative_interest_repayment_myr = source.acc_cumulative_interest_repayment_myr
            target.acc_tawidh_payment_repayment_fc = source.acc_tawidh_payment_repayment_fc,
            target.acc_tawidh_payment_repayment_myr = source.acc_tawidh_payment_repayment_myr,
            target.acc_cumulative_tawidh_payment_repayment_fc = source.acc_cumulative_tawidh_payment_repayment_fc,
            target.acc_cumulative_tawidh_payment_repayment_myr = source.acc_cumulative_tawidh_payment_repayment_myr,
            target.acc_others_charges_payment_fc = source.acc_others_charges_payment_fc,
            target.acc_others_charges_payment_myr = source.acc_others_charges_payment_myr,
            target.acc_cumulative_others_charge_payment_fc = source.acc_cumulative_others_charge_payment_fc,
            target.acc_cumulative_others_charge_payment_myr = source.acc_cumulative_others_charge_payment_myr;
    """, (reportingDate,))
    conn.commit() 

    # # incase manual
    # cursor.execute("""MERGE INTO dbase_account_hist AS target USING A_PROFIT_N_OTHER_PAYMENT AS source
    # ON target.finance_sap_number = source.Account
    # WHEN MATCHED AND target.position_as_at = '2025-05-31' THEN
    #     UPDATE SET target.penalty_repayment = source.acc_tawidh_payment_repayment_fc,
    #             target.penalty_repayment_myr = source.acc_tawidh_payment_repayment_myr,
    #             target.cumulative_penalty = source.acc_cumulative_tawidh_payment_repayment_fc,
    #             target.cumulative_penalty_myr = source.acc_cumulative_tawidh_payment_repayment_myr,
    #             target.other_charges_payment = source.acc_others_charges_payment_fc,
    #             target.other_charges_payment_myr = source.acc_others_charges_payment_myr,
    #             target.cumulative_other_charges_payment = source.acc_cumulative_others_charge_payment_fc,
    #             target.cumulative_other_charges_payment_myr = source.acc_cumulative_others_charge_payment_myr;
    # """)
    # conn.commit() 

    #
		#target.acc_interest_repayment_fc = source.acc_interest_repayment_fc,
                #target.acc_interest_repayment_myr = source.acc_interest_repayment_myr,
                #target.acc_cumulative_interest_repayment_fc = source.acc_cumulative_interest_repayment_fc,
                #target.acc_cumulative_interest_repayment_myr = source.acc_cumulative_interest_repayment_myr,

    #WHEN NOT MATCHED THEN
    #            target.acc_interest_repayment_fc = 0,
    #            target.acc_interest_repayment_myr = 0,
    #            target.acc_cumulative_interest_repayment_fc = 0,
    #            target.acc_cumulative_interest_repayment_myr = 0,
    #            target.acc_others_charges_payment_fc = 0,
    #            target.acc_others_charges_payment_myr = 0,
    #            target.acc_cumulative_others_charge_payment_fc = 0,
    #            target.acc_cumulative_others_charge_payment_myr = 0,
    #            target.acc_tawidh_payment_repayment_fc = 0,
    #            target.acc_tawidh_payment_repayment_myr = 0,
    #            target.acc_cumulative_tawidh_payment_repayment_fc = 0,
    #            target.acc_cumulative_tawidh_payment_repayment_myr = 0;

    #         BY TARGET
    # INSERT (finance_sap_number,
    #               acc_interest_repayment_fc,
    #               acc_interest_repayment_myr,
    #               acc_cumulative_interest_repayment_fc,
    #               acc_cumulative_interest_repayment_myr,
    #               acc_others_charges_payment_fc,
    #               acc_others_charges_payment_myr,
    #               acc_cumulative_others_charge_payment_fc,
    #               acc_cumulative_others_charge_payment_myr,
    #               acc_tawidh_payment_repayment_fc,
    #               acc_tawidh_payment_repayment_myr,
    #               acc_cumulative_tawidh_payment_repayment_fc,
    #               acc_cumulative_tawidh_payment_repayment_myr,
    #               facility_exim_account_num)
    #    VALUES (source.Account,0,0,0,0,0,0,0,0,0,0,0,0,ISNULL(source.Account,'default_value'))
    
    cursor.execute("drop table A_PROFIT_N_OTHER_PAYMENT")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_query4)
    conn.commit() 


        #table        
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Result_Data_Mirror_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')] #cari pakai code jgn pakai id ,36978,36960
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

    create_table_query_result = "CREATE TABLE A_download_result_B (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_result.iterrows():
        sql_result = "INSERT INTO A_download_result_B({}) VALUES ({})".format(','.join(download_result.columns), ','.join(['?']*len(download_result.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()


    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_result_B AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);      
    """)
    conn.commit() 
    cursor.execute("drop table A_download_result_B")
    conn.commit() 


    print("Data updated successfully at "+str(current_time))
    conn.close()
except Exception as e:
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
    cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database Data Mirror",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Update Database Data Mirror'
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Update Database Data Mirror Error: {e}")
    sys.exit(f"Update Database Data Mirror Error: {str(e)}")

    #==============================================================================================

    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Not Applicable",'PY004','PY004')] #,36961,36961
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


#smpi sini 20241126
#1. update log jobpython refer param_system_param
#1. amik param dr java

#jobid auto generate
#jobname taruk nme script
#jobstatus == param_id
#jobdate realtime
#jobdate realtime#

#ad param_id bru tau status jobphyton tu kat mne (param_system_param)

#SELECT TOP (1000) [jobId]
#      ,[jobName]
#      ,[jobStatus]
#      ,[jobStartDate]
#      ,[jobCompleted]
#  FROM [mis_db_prod_backup_2024_04_02].[dbo].[jobPhyton]

#SELECT TOP (1000) [param_id]
#      ,[param_id_vc]
#      ,[param_code]
#      ,[param_name]
#      ,[parent_param_id]
#      ,[param_level]
#      ,[param_reference]
#      ,[param_root_id]
#      ,[param_haircut_percent]
#      ,[param_others_code]
#      ,[param_rating]
#  FROM [mis_db_prod_backup_2024_04_02].[dbo].[param_system_param] where param_reference like '%python%'
#--------------------------------------------------------------Sandbox----------------------------------------------------------------------------------------------------------------------------------------

#df = pd.read_sql_query("select * from col_facilities_application_master", conn)

#cursor.execute("CREATE TABLE AA (column1 object)")
#df = pd.read_sql_query("select * from AA", conn)

# Create a metadata object
#metadata = MetaData()

# Create a session
#Session = sessionmaker(bind=engine)
#session = Session()

#combine2.to_sql("PROFIT_N_OTHER_PAYMENT", engine, if_exists="replace", index=False) #if_exists=append

#PROFIT_N_OTHER_PAYMENT = pd.read_sql("SELECT * FROM PROFIT_N_OTHER_PAYMENT", con=engine)

# Reflect the table from the database
#PROFIT_N_OTHER_PAYMENT = Table('PROFIT_N_OTHER_PAYMENT', metadata, autoload_with=engine)
#LDB_prev = Table('col_facilities_application_master', metadata, autoload_with=engine)


#from sqlalchemy import text

#merge_query = """
#MERGE INTO col_facilities_application_master AS target
#USING PROFIT_N_OTHER_PAYMENT AS source
#ON target.finance_sap_number = source.Account
#WHEN MATCHED THEN
#    UPDATE SET target.acc_interest_repayment_fc = source.acc_interest_repayment_fc,
#               target.acc_interest_repayment_myr = source.acc_interest_repayment_myr,
#               target.acc_cumulative_interest_repayment_fc = source.acc_cumulative_interest_repayment_fc,
#               target.acc_cumulative_interest_repayment_myr = source.acc_cumulative_interest_repayment_myr
#WHEN NOT MATCHED BY TARGET THEN
#    INSERT (finance_sap_number,acc_interest_repayment_fc,acc_interest_repayment_myr,acc_cumulative_interest_repayment_fc,acc_cumulative_interest_repayment_myr, facility_exim_account_num)
#    VALUES (source.Account,0,0,0,0, ISNULL(source.Account, 'default_value'));
#"""

#combine2.iloc[np.where(combine2.Account=="500605")]

# Execute the MERGE statement
#with engine.connect() as connection:
#    connection.execute(text(merge_query))


# Create the update statement
#stmt = (
#    update(col_facilities_application_master)
#    .values({
#        col_facilities_application_master.c.acc_interest_repayment_fc : PROFIT_N_OTHER_PAYMENT.c.acc_interest_repayment_fc
#    }).where(col_facilities_application_master.c.finance_sap_number==PROFIT_N_OTHER_PAYMENT.c.Account)
#    )

#session.execute(merge_query)
#session.commit()


# Define the table you want to drop
#my_table = Table('PROFIT_N_OTHER_PAYMENT', metadata)

# Drop the table
#my_table.drop(engine)

#acc_interest_repayment_fc
#acc_interest_repayment_myr
#acc_cumulative_interest_repayment_fc
#acc_cumulative_interest_repayment_myr

#CRUD = create read update delete

#test = pd.read_sql_query("SELECT * FROM AA", conn)

#cursor.execute("CREATE TABLE AA ({})".format(','.join(combine2.columns)))

#cursor.execute("CREATE TABLE IF NOT EXISTS AA ({})".format(','.join(combine2.columns)))

#cursor.execute("drop table AA")
#conn.commit() 
#conn.close()

#cursor.execute('SELECT TOP 10 * FROM col_facilities_application_master')
#for row in cursor:*
#    print('row = %r' % (row,))

#df = pd.read_sql_query("select finance_sap_number, acc_drawdown_fc,acc_drawdown_myr,acc_cumulative_drawdown,acc_cumulative_drawdown_myr,acc_repayment_fc,acc_repayment_myr,
# acc_cumulative_repayment_myr from col_facilities_application_master", conn)


#df.shape
#df.to_excel("test.xlsx",index=False)

#import urllib
#quoted = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.32.1.51,1455;")
#engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

#df_from_db = pd.read_sql("SELECT * FROM PROFIT_N_OTHER_PAYMENT", con=engine)