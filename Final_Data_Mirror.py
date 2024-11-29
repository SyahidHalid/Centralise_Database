# python Final_Data_Mirror.py "0","Data Mirror October 2024.xlsx","Data Mirror","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python Final_Data_Mirror.py "0" "Data Mirror October 2024.xlsx" "Data Mirror" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-03-29"
# position_as_at

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
        sys.exit(1)
 
# Function to update user data
def set_user(connection, documentId, documentName, jobName, statusName, uploadedById, uploadedByEmail, reportingDate):
    print("Starting user update...")
    try:
        # Open a cursor to interact with the database
        with connection.cursor() as cursor:
            # Update the user data in the 'users' table
            cursor.execute(
                "UPDATE users SET username = ? WHERE userId = ?",
                ('rozaimizamahriMISPYTHON', 1)
            )
            # Commit the changes
            connection.commit()
 
        print("User updated successfully.")
    except Exception as e:
        print(f"Error updating user: {e}")

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
        set_user(connection, documentId, documentName, jobName, statusName, uploadedById, uploadedByEmail, reportingDate)
 
    except Exception as e:
        print(f"Script failed with exception: {e}")
        sys.exit(1)  # Exit the script with a failure code
    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
            print("Database connection closed.")

try:
    #   Library
    import pandas as pd
    import numpy as np
    import pyodbc
    import datetime as dt
    from sqlalchemy import create_engine
    from sqlalchemy import Table, MetaData
    from sqlalchemy import update
    from sqlalchemy.orm import sessionmaker
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
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Data_Mirror.py',[jobCompleted] = NULL
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_query1)
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")


#upload excel
try:
    #   Excel File Name

    #E:mis_doc\\PythonProjects\\misPython\\misPython_doc
    #df1 = documentName #"Data Mirror October 2024.xlsx"
    #import config
    #documentName = "ECL 1024 - MIS v1.xlsx"
    
    df1 =  str(config.FOLDER_CONFIG["FTP_directory"]+documentName) #"ECL 1024 - MIS v1.xlsx" #documentName

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
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Upload Excel Data Mirror'
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_error)
    conn.commit() 



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

    merge1 = pd.concat([Interest1,IIS1,Profit1,PIS1]).fillna(0).rename(columns={'___Amt_in_loc_cur_':'Profit_Payment_Interest_Repayment_MYR','______Amount_in_DC':'Profit_Payment_Interest_Repayment_FC'})

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


    #combine2
    combine2 = combine.drop(['acc_tawidh_payment_repayment_fc',
                            'acc_tawidh_payment_repayment_myr',
                            'acc_cumulative_tawidh_payment_repayment_fc',
                            'acc_cumulative_tawidh_payment_repayment_myr'],axis=1).merge(Tawidh_Comb1,on="Account", how="outer").fillna(0) #,indicator=True

    convert_time = str(current_time).replace(":","-")

    combine2['position_as_at'] = reportingDate

    combine2.to_excel(config.FOLDER_CONFIG["FTP_directory"]+"Result_Data_Mirror_"+str(convert_time)[:19]+".xlsx",index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

    #combine2.to_excel("Data_Mirror_"+str(convert_time)[:19]+".xlsx",index=False)
    #df1 =  config.FOLDER_CONFIG["FTP_directory"]+documentName #"ECL 1024 - MIS v1.xlsx" #documentName
    
    #combine2.dtypes
    #combine2.shape
    #combine2.to_excel('05. Profit Payment & Other Charges Payment.xlsx', index=False)
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
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Process Excel Data Mirror'
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_error)
    conn.commit() 


#--------------------------------------------------------connect ngan database-----------------------------------------------------------------------------------------------------------------------------------------------------

#smpi sini
#current_time = pd.Timestamp.now()

try:
    # Assuming 'combine2' is a DataFrame
    column_types = []
    for col in combine2.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if combine2[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif combine2[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif combine2[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others


    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_PROFIT_N_OTHER_PAYMENT (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in combine2.iterrows():
        sql = "INSERT INTO A_PROFIT_N_OTHER_PAYMENT({}) VALUES ({})".format(','.join(combine2.columns), ','.join(['?']*len(combine2.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_PROFIT_N_OTHER_PAYMENT AS source
    ON target.finance_sap_number = source.Account
    WHEN MATCHED THEN
        UPDATE SET target.acc_interest_repayment_fc = source.acc_interest_repayment_fc,
                target.acc_interest_repayment_myr = source.acc_interest_repayment_myr,
                target.acc_cumulative_interest_repayment_fc = source.acc_cumulative_interest_repayment_fc,
                target.acc_cumulative_interest_repayment_myr = source.acc_cumulative_interest_repayment_myr,
                target.acc_others_charges_payment_fc = source.acc_others_charges_payment_fc,
                target.acc_others_charges_payment_myr = source.acc_others_charges_payment_myr,
                target.acc_cumulative_others_charge_payment_fc = source.acc_cumulative_others_charge_payment_fc,
                target.acc_cumulative_others_charge_payment_myr = source.acc_cumulative_others_charge_payment_myr,
                target.acc_tawidh_payment_repayment_fc = source.acc_tawidh_payment_repayment_fc,
                target.acc_tawidh_payment_repayment_myr = source.acc_tawidh_payment_repayment_myr,
                target.acc_cumulative_tawidh_payment_repayment_fc = source.acc_cumulative_tawidh_payment_repayment_fc,
                target.acc_cumulative_tawidh_payment_repayment_myr = source.acc_cumulative_tawidh_payment_repayment_myr,
                target.position_as_at = source.position_as_at;
    """)
    conn.commit() 


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
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002'
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_query4)
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
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Update Database Data Mirror'
    WHERE [jobName] = 'Data Mirror';
                """
    cursor.execute(sql_error)
    conn.commit() 

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