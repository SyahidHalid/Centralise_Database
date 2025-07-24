# python Disbursement_Repayment.py 10,"Disbursement & Repayment October 2024.xlsx","Disbursement & Repayment","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python Disbursement_Repayment.py 10 "Disbursement&RepaymentMay2025.xlsx.xlsx.xlsx.xlsx" "Disbursement & Repayment" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-05-31"
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
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Disbursement_Repayment.py', [jobCompleted] = NULL
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_query1)
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")
    sys.exit(f"Connect to Database Error: {str(e)}")
    #sys.exit(1)
        
#----------------------------------------------------------------------------------------------------

#process
try:
    #    #E:\PythonProjects\misPython\misPython_doc
    # documentName = "DisbursementRepaymentJune2025.xlsx.xlsx"
    # reportingDate = "2025-06-30"
    df1 =  os.path.join(config.FOLDER_CONFIG["FTP_directory"],documentName) #"ECL 1024 - MIS v1.xlsx" #documentName

    D1 = "Disbursement Islamic"
    D2 = "Repayment Islamic"
    D3 = "Disbursement Conventional"
    D4 = "Repayment Conventional"


    Dis_isl = pd.read_excel(df1, sheet_name=D1, header=8)
    Rep_Isl = pd.read_excel(df1, sheet_name=D2, header=8)
    Dis_Conv = pd.read_excel(df1, sheet_name=D3, header=8)
    Rep_Conv = pd.read_excel(df1, sheet_name=D4, header=8)
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
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","Upload Excel Disbursement & Repayment",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY003', [jobErrDetail]= 'Upload Excel Disbursement & Repayment'
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Upload Excel Disbursement & Repayment Error: {e}")
    sys.exit(f"Upload Excel Disbursement & Repayment Error: {str(e)}")
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
    Dis_isl.columns = Dis_isl.columns.str.replace("\n", "_")
    Dis_isl.columns = Dis_isl.columns.str.replace(" ", "")

    Rep_Isl.columns = Rep_Isl.columns.str.replace("\n", "_")
    Rep_Isl.columns = Rep_Isl.columns.str.replace(" ", "")

    Dis_Conv.columns = Dis_Conv.columns.str.replace("\n", "_")
    Dis_Conv.columns = Dis_Conv.columns.str.replace(" ", "")

    Rep_Conv.columns = Rep_Conv.columns.str.replace("\n", "_")
    Rep_Conv.columns = Rep_Conv.columns.str.replace(" ", "")

    Dis_isl['St'] =   Dis_isl['St'].str[8:]
    Rep_Isl['St'] =   Rep_Isl['St'].str[8:]
    Dis_Conv['St'] =   Dis_Conv['St'].str[8:]
    Rep_Conv['St'] =   Rep_Conv['St'].str[8:]

    Dis_isl.rename(columns={'St':'Account'},inplace=True)
    Rep_Isl.rename(columns={'St':'Account'},inplace=True)
    Dis_Conv.rename(columns={'St':'Account'},inplace=True)
    Rep_Conv.rename(columns={'St':'Account'},inplace=True)



    Dis_isl_1 = Dis_isl.iloc[np.where((Dis_isl['Unnamed:1']=="**")&(~Dis_isl.Account.isin(['Account']))&~(Dis_isl.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()
    Rep_Isl_1 = Rep_Isl.iloc[np.where((Rep_Isl['Unnamed:1']=="**")&(~Rep_Isl.Account.isin(['Account']))&~(Rep_Isl.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()
    Dis_Conv_1 = Dis_Conv.iloc[np.where((Dis_Conv['Unnamed:1']=="**")&(~Dis_Conv.Account.isin(['Account']))&~(Dis_Conv.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()
    Rep_Conv_1 = Rep_Conv.iloc[np.where((Rep_Conv['Unnamed:1']=="**")&(~Rep_Conv.Account.isin(['Account']))&~(Rep_Conv.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()

    Dis_isl_1['Type_of_Financing'] = 'Islamic'
    Rep_Isl_1['Type_of_Financing'] = 'Islamic'
    Dis_Conv_1['Type_of_Financing'] = 'Conventional'
    Rep_Conv_1['Type_of_Financing'] = 'Conventional'

    #st.write(Dis_isl_1)

    Disbursement = pd.concat([Dis_isl_1,Dis_Conv_1])
    Repayment = pd.concat([Rep_Isl_1,Rep_Conv_1])

    Disbursement.rename(columns={'AmountinDC': 'Disbursement_Drawdown_Facility_Currency',
                                'Amtinloc.cur.':'Disbursement_Drawdown_MYR'},inplace=True)

    Repayment.rename(columns={'AmountinDC': 'Cost_Payment_Principal_Repayment_Facility_Currency',
                            'Amtinloc.cur.':'Cost_Payment_Principal_Repayment_MYR'},inplace=True)

    Repayment['Cost_Payment_Principal_Repayment_Facility_Currency'] = -1*Repayment['Cost_Payment_Principal_Repayment_Facility_Currency']
    Repayment['Cost_Payment_Principal_Repayment_MYR'] = -1*Repayment['Cost_Payment_Principal_Repayment_MYR']

    merge = Disbursement.fillna(0).merge(Repayment.fillna(0),on=['Account','Curr.','Type_of_Financing'],how='outer')
    merge.fillna(0, inplace=True)

    merge['Account'] = merge['Account'].astype(str)

    #------------------------------------------------Loan Database--------------------------------------

    #dlm db finance_sap_number
    #dlm db acc_cumulative_drawdown
    #dlm db acc_drawdown_fc

    LDB_prev['finance_sap_number'] = LDB_prev['finance_sap_number'].astype(str)

    LDB_prev.columns = LDB_prev.columns.str.replace("\n", "")

    LDB_prev['acc_drawdown_fc'].fillna(0,inplace=True)
    LDB_prev['acc_drawdown_myr'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_drawdown'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_drawdown_myr'].fillna(0,inplace=True)
    LDB_prev['acc_repayment_fc'].fillna(0,inplace=True)
    LDB_prev['acc_repayment_myr'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_repayment'].fillna(0,inplace=True)
    LDB_prev['acc_cumulative_repayment_myr'].fillna(0,inplace=True)

    appendfinal_ldb = merge.merge(LDB_prev.iloc[np.where(~(LDB_prev['finance_sap_number'].isna()))][['finance_sap_number',
                                                                                                    'acc_drawdown_fc',
                                                                                                    'acc_drawdown_myr',
                                                                                                    'acc_cumulative_drawdown',
                                                                                                    'acc_cumulative_drawdown_myr',
                                                                                                    'acc_repayment_fc',
                                                                                                    'acc_repayment_myr',
                                                                                                    'acc_cumulative_repayment',
                                                                                                    'acc_cumulative_repayment_myr']].drop_duplicates('finance_sap_number',keep='first').rename(columns={'finance_sap_number':'Account'}),on=['Account'],how='left',suffixes=('_x', ''),indicator=True)

    #appendfinal_ldb.head(1)
    #merge.head(1)

    #---------------------------------------------------------------------------------------------------

    appendfinal_ldb['Disbursement_Drawdown_Facility_Currency'].fillna(0,inplace=True)
    appendfinal_ldb['Disbursement_Drawdown_MYR'].fillna(0,inplace=True)
    appendfinal_ldb['Cost_Payment_Principal_Repayment_Facility_Currency'].fillna(0,inplace=True) 
    appendfinal_ldb['Cost_Payment_Principal_Repayment_MYR'].fillna(0,inplace=True)

    appendfinal_ldb['acc_cumulative_drawdown'].fillna(0,inplace=True)
    appendfinal_ldb['acc_cumulative_drawdown_myr'].fillna(0,inplace=True)
    appendfinal_ldb['acc_cumulative_repayment'].fillna(0,inplace=True) 
    appendfinal_ldb['acc_cumulative_repayment_myr'].fillna(0,inplace=True)

    appendfinal2 = appendfinal_ldb#.fillna(0)

    appendfinal2['Cumulative_Disbursement_Drawdown_Facility_Currency'] = appendfinal2['Disbursement_Drawdown_Facility_Currency'] +  appendfinal2['acc_cumulative_drawdown'] 
    appendfinal2['Cumulative_Disbursement_Drawdown_MYR'] = appendfinal2['Disbursement_Drawdown_MYR'] +  appendfinal2['acc_cumulative_drawdown_myr'] 

    appendfinal2['Cumulative_Cost_Payment_Principal_Repayment_Facility_Currency'] = appendfinal2['Cost_Payment_Principal_Repayment_Facility_Currency'] +  appendfinal2['acc_cumulative_repayment'] 
    appendfinal2['Cumulative_Cost_Payment_Principal_Repayment_MYR'] = appendfinal2['Cost_Payment_Principal_Repayment_MYR'] +  appendfinal2['acc_cumulative_repayment_myr'] 

    appendfinal2.sort_values('Disbursement_Drawdown_MYR', ascending=False, inplace=True)

    appendfinal3 = appendfinal2[['Account',
    'Disbursement_Drawdown_Facility_Currency',
    'Disbursement_Drawdown_MYR',
    'Cumulative_Disbursement_Drawdown_Facility_Currency',
    'Cumulative_Disbursement_Drawdown_MYR',
    'Cost_Payment_Principal_Repayment_Facility_Currency',
    'Cost_Payment_Principal_Repayment_MYR',
    'Cumulative_Cost_Payment_Principal_Repayment_Facility_Currency',
    'Cumulative_Cost_Payment_Principal_Repayment_MYR']].rename(columns={'Disbursement_Drawdown_Facility_Currency':'acc_drawdown_fc',
                                                                        'Disbursement_Drawdown_MYR':'acc_drawdown_myr',
                                                                        'Cumulative_Disbursement_Drawdown_Facility_Currency':'acc_cumulative_drawdown',
                                                                        'Cumulative_Disbursement_Drawdown_MYR':'acc_cumulative_drawdown_myr',
                                                                        'Cost_Payment_Principal_Repayment_Facility_Currency':'acc_repayment_fc',
                                                                        'Cost_Payment_Principal_Repayment_MYR':'acc_repayment_myr',
                                                                        'Cumulative_Cost_Payment_Principal_Repayment_Facility_Currency':'acc_cumulative_repayment',
                                                                        'Cumulative_Cost_Payment_Principal_Repayment_MYR':'acc_cumulative_repayment_myr'})
    
    convert_time = str(current_time).replace(":","-")
    appendfinal3['position_as_at'] = reportingDate

    df_add_Humm = pd.DataFrame([['500776A',
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0]], columns=['Account',
                                              'acc_drawdown_fc',
                                              'acc_drawdown_myr',
                                              'acc_cumulative_drawdown',
                                              'acc_cumulative_drawdown_myr',
                                              'acc_repayment_fc',
                                              'acc_repayment_myr',
                                              'acc_cumulative_repayment',
                                              'acc_cumulative_repayment_myr'])

    appendfinal3 = pd.concat([appendfinal3, df_add_Humm])

    a_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_drawdown_fc'])
    b_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_drawdown_myr'])
    c_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_cumulative_drawdown'])
    d_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_cumulative_drawdown_myr'])
    e_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_repayment_fc'])
    f_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_repayment_myr'])
    g_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_cumulative_repayment'])
    h_humm = sum(appendfinal3.fillna(0).iloc[np.where(appendfinal3['Account']=='500776')]['acc_cumulative_repayment_myr'])

    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_drawdown_fc'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_drawdown_myr'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_cumulative_drawdown'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_cumulative_drawdown_myr'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_repayment_fc'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_repayment_myr'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_cumulative_repayment'] = 0.79*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776'),'acc_cumulative_repayment_myr'] = 0.79*a_humm

    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_drawdown_fc'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_drawdown_myr'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_cumulative_drawdown'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_cumulative_drawdown_myr'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_repayment_fc'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_repayment_myr'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_cumulative_repayment'] = 0.21*a_humm
    appendfinal3.loc[(appendfinal3['Account']=='500776A'),'acc_cumulative_repayment_myr'] = 0.21*a_humm

    LDB_cum = pd.read_sql_query("SELECT * FROM dbase_account_hist;", conn)
    
    reportingYear = int(reportingDate[:4])

    # Convert to datetime
    LDB_cum['position_as_at'] = pd.to_datetime(LDB_cum['position_as_at'], errors='coerce')

    # Filter by year
    LDB_cum_filtered = LDB_cum.loc[(LDB_cum['position_as_at'].dt.year == reportingYear)&(LDB_cum['position_as_at']<reportingDate)]

    LDB_cum_filtered['acc_drawdown_fc'] = LDB_cum_filtered['acc_drawdown_fc'].astype(float)
    LDB_cum_filtered['acc_drawdown_myr'] = LDB_cum_filtered['acc_drawdown_myr'].astype(float)
    LDB_cum_filtered['acc_repayment_fc'] = LDB_cum_filtered['acc_repayment_fc'].astype(float)
    LDB_cum_filtered['acc_repayment_myr'] = LDB_cum_filtered['acc_repayment_myr'].astype(float)

    LDB_cum_group = LDB_cum_filtered.iloc[np.where(~(LDB_cum_filtered.finance_sap_number.isna())&
                                                   (LDB_cum_filtered.finance_sap_number!='')&
                                                   (LDB_cum_filtered.finance_sap_number!='NEW ACCOUNT'))].groupby(['finance_sap_number',
                                                                                                                   'cif_name'])[['acc_drawdown_fc',
                                                                      'acc_drawdown_myr',
                                                                      'acc_repayment_fc',
                                                                      'acc_repayment_myr']].sum().reset_index()

    LDB_cum_group.rename(columns={'acc_drawdown_fc':'acc_cumulative_drawdown',
                                  'acc_drawdown_myr':'acc_cumulative_drawdown_myr',
                                  'acc_repayment_fc':'acc_cumulative_repayment',
                                  'acc_repayment_myr':'acc_cumulative_repayment_myr'},inplace=True)
    
    appendfinal4 = LDB_cum_group.merge(appendfinal3[['Account',
                                             'acc_drawdown_fc',
                                             'acc_drawdown_myr',
                                             'acc_repayment_fc',
                                             'acc_repayment_myr',
                                             'position_as_at']].rename(columns={'Account':'finance_sap_number'}),
                                             on='finance_sap_number',
                                             how='outer',
                                             indicator=True)
    
    appendfinal4['acc_cumulative_drawdown'] = appendfinal4['acc_drawdown_fc'].fillna(0) + appendfinal4['acc_cumulative_drawdown'].fillna(0)
    appendfinal4['acc_cumulative_drawdown_myr'] = appendfinal4['acc_drawdown_myr'].fillna(0) + appendfinal4['acc_cumulative_drawdown_myr'].fillna(0)
    appendfinal4['acc_cumulative_repayment'] = appendfinal4['acc_repayment_fc'].fillna(0) + appendfinal4['acc_cumulative_repayment'].fillna(0)
    appendfinal4['acc_cumulative_repayment_myr'] = appendfinal4['acc_repayment_myr'].fillna(0) + appendfinal4['acc_cumulative_repayment_myr'].fillna(0)

    appendfinal4 = appendfinal4[['finance_sap_number','cif_name','_merge',
                         'acc_drawdown_fc','acc_drawdown_myr','acc_cumulative_drawdown','acc_cumulative_drawdown_myr',
                         'acc_repayment_fc','acc_repayment_myr','acc_cumulative_repayment','acc_cumulative_repayment_myr',
                       'position_as_at']]#.fillna(0)#.sort_values(by='_merge',ascending=True)

    appendfinal4['position_as_at'] = reportingDate
    appendfinal4['cif_name'].fillna('Only Exist in FAD',inplace=True)
    appendfinal4['acc_drawdown_fc'].fillna(0,inplace=True)
    appendfinal4['acc_drawdown_myr'].fillna(0,inplace=True)
    appendfinal4['acc_cumulative_drawdown'].fillna(0,inplace=True)
    appendfinal4['acc_cumulative_drawdown_myr'].fillna(0,inplace=True)
    appendfinal4['acc_repayment_fc'].fillna(0,inplace=True)
    appendfinal4['acc_repayment_myr'].fillna(0,inplace=True)
    appendfinal4['acc_cumulative_repayment'].fillna(0,inplace=True)
    appendfinal4['acc_cumulative_repayment_myr'].fillna(0,inplace=True)


    #appendfinal4.head(1)


    #======================================with exception report

    #appendfinal3.head(1)
    #LDB_hist = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = '{reportingDate}';", conn)

    # LDB_hist.position_as_at.value_counts()
    # LDB_hist1 = LDB_hist.iloc[np.where((~LDB_hist.finance_sap_number.isna())&((LDB_hist.acc_drawdown_fc>0)|(LDB_hist.acc_repayment_fc>0)))]
    
    LDB_name = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = ?;", conn, params=(reportingDate,))
   
    LDB_hist_before = pd.read_sql_query("SELECT * FROM col_facilities_application_master where position_as_at = ?;", conn, params=(reportingDate,))
   
    LDB_hist = LDB_hist_before.merge(LDB_name[['finance_sap_number','cif_name']], on='finance_sap_number', how='left')
    
    condition1 = ~LDB_hist.finance_sap_number.isna()
    condition2 = (LDB_hist.acc_drawdown_fc > 0) | (LDB_hist.acc_repayment_fc > 0)

    # LDB_hist.head(1)
    LDB_hist1 = LDB_hist[['finance_sap_number',
                                                                  'cif_name',
                                                   'acc_drawdown_fc',
                                                   'acc_drawdown_myr',
                                                   'acc_repayment_fc',
                                                   'acc_repayment_myr']] #.iloc[np.where(condition1 & condition2)]
 
    # LDB_hist1 = LDB_hist[['finance_sap_number',
    #                       'acc_drawdown_fc',
    #                       'acc_drawdown_myr',
    #                       'acc_repayment_fc',
    #                       'acc_repayment_myr']]
    
    exception_report = appendfinal3.rename(columns={'Account':'finance_sap_number'}).merge(LDB_hist1, on='finance_sap_number', how='outer', suffixes=('_Sap','_Mis'),indicator=True)

    # exception_report.head(1)

    #Disbursement/Drawdown (Facility Currency)
    exception_report["diff_drawdown_fc"] = exception_report["acc_drawdown_fc_Sap"].fillna(0) - exception_report["acc_drawdown_fc_Mis"].fillna(0)
    
    #Disbursement/Drawdown (MYR)
    exception_report["diff_drawdown_myr"] = exception_report["acc_drawdown_myr_Sap"].fillna(0) - exception_report["acc_drawdown_myr_Mis"].fillna(0)
    
    #Cost Payment/Principal Repayment (Facility Currency)
    exception_report["diff_cost_payment_fc"] = exception_report["acc_repayment_fc_Sap"].fillna(0) - exception_report["acc_repayment_fc_Mis"].fillna(0)
    
    #Cost Payment/Principal Repayment (MYR)
    exception_report["diff_cost_payment_myr"] = exception_report["acc_repayment_myr_Sap"].fillna(0) - exception_report["acc_repayment_myr_Mis"].fillna(0)
    
    exception_report.position_as_at.fillna(reportingDate,inplace=True)
    
    exception_report1 = exception_report[['finance_sap_number',
                                          'cif_name',
                                          'position_as_at',
                                          '_merge',
                                          'acc_drawdown_fc_Sap',
                                          'acc_drawdown_fc_Mis',
                                          'diff_drawdown_fc',
                                          'acc_drawdown_myr_Sap',
                                          'acc_drawdown_myr_Mis',
                                          'diff_drawdown_myr',
                                          'acc_repayment_fc_Sap',
                                          'acc_repayment_fc_Mis',
                                          'diff_cost_payment_fc',
                                          'acc_repayment_myr_Sap',
                                          'acc_repayment_myr_Mis',
                                          'diff_cost_payment_myr']]

    # exception_report.loc[exception_report._merge=='left_only','_merge'] = 'sap_only'
    # exception_report.loc[exception_report._merge=='right_only','_merge'] = 'mis_only'

    #exception_report._merge.value_counts()


    # cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_DIS_N_REPAYMENT AS source
    # ON target.finance_sap_number = source.Account
    # WHEN MATCHED THEN
    #     UPDATE SET target.position_as_at = source.position_as_at;
    # """)
    # conn.commit() 

    #redundant
    #target.acc_drawdown_fc = source.acc_drawdown_fc,
    #            target.acc_drawdown_myr = source.acc_drawdown_myr,
    #            target.acc_cumulative_drawdown = source.acc_cumulative_drawdown,
    #            target.acc_cumulative_drawdown_myr = source.acc_cumulative_drawdown_myr,
    #target.acc_repayment_fc = source.acc_repayment_fc,
    #            target.acc_repayment_myr = source.acc_repayment_myr,
    #            target.acc_cumulative_repayment = source.acc_cumulative_repayment,
    #            target.acc_cumulative_repayment_myr = source.acc_cumulative_repayment_myr,

    # Extract
    writer2 = pd.ExcelWriter(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_Disbursement_Repayment_"+str(convert_time)[:19]+".xlsx"),engine='xlsxwriter')

    appendfinal4.to_excel(writer2, sheet_name='Result', index = False)

    exception_report1.to_excel(writer2, sheet_name='Exception', index = False)

    writer2.close()



    #appendfinal3.to_excel(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_Disbursement_Repayment_"+str(convert_time)[:19]+".xlsx"),index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

    #appendfinal3.to_excel("Disbursement_Repayment_"+str(convert_time)[:19]+".xlsx",index=False)
    #df1 =  config.FOLDER_CONFIG["FTP_directory"]+documentName #"ECL 1024 - MIS v1.xlsx" #documentName

    cursor.execute("DROP TABLE IF EXISTS Exception_Disbursement_Repayment")
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
    create_table_query_result = "CREATE TABLE Exception_Disbursement_Repayment (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in exception_report1.iterrows():
        sql_result = "INSERT INTO Exception_Disbursement_Repayment({}) VALUES ({})".format(','.join(exception_report1.columns), ','.join(['?']*len(exception_report1.columns)))
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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel Disbursement & Repayment",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY003', [jobErrDetail]= '"Process Excel Disbursement & Repayment'
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_error)
    conn.commit() 
    print(f"Process Excel Disbursement & Repayment Error: {e}")
    sys.exit(f"Process Excel Disbursement & Repayment Error: {str(e)}")
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
    data = [(documentId,"Result_Disbursement_Repayment_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')]
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

    create_table_query_result = "CREATE TABLE A_download_result_C (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_result.iterrows():
        sql_result = "INSERT INTO A_download_result_C({}) VALUES ({})".format(','.join(download_result.columns), ','.join(['?']*len(download_result.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()


    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_result_C AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);   
    """)
    conn.commit() 
    cursor.execute("drop table A_download_result_C")
    conn.commit() 

    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #appendfinal3.position_as_at.fillna(reportingDate,inplace=True)
    
    # appendfinal4.head(1)
    # appendfinal4.iloc[np.where(appendfinal4.position_as_at=='')]
    
    # Assuming 'appendfinal4' is a DataFrame
    column_types = []
    for col in appendfinal4.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if appendfinal4[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif appendfinal4[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif appendfinal4[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others


    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_DIS_N_REPAYMENT (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in appendfinal4.iterrows():
        sql = "INSERT INTO A_DIS_N_REPAYMENT({}) VALUES ({})".format(','.join(appendfinal4.columns), ','.join(['?']*len(appendfinal4.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()
   
    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_DIS_N_REPAYMENT AS source
    ON target.finance_sap_number = source.finance_sap_number
    WHEN MATCHED AND target.position_as_at = ? THEN
        UPDATE SET target.acc_drawdown_myr = source.acc_drawdown_myr,
                target.acc_cumulative_drawdown_myr = source.acc_cumulative_drawdown_myr,
                target.acc_repayment_myr = source.acc_repayment_myr,
                target.acc_cumulative_repayment_myr = source.acc_cumulative_repayment_myr;
    """, (reportingDate,))
    conn.commit() 
    # cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_DIS_N_REPAYMENT AS source
    # ON target.finance_sap_number = source.Account
    # WHEN MATCHED THEN
    #     UPDATE SET target.position_as_at = source.position_as_at;
    # """)
    # conn.commit() 

    #redundant
    #target.acc_drawdown_fc = source.acc_drawdown_fc,
    #            target.acc_drawdown_myr = source.acc_drawdown_myr,
    #            target.acc_cumulative_drawdown = source.acc_cumulative_drawdown,
    #            target.acc_cumulative_drawdown_myr = source.acc_cumulative_drawdown_myr,
    #target.acc_repayment_fc = source.acc_repayment_fc,
    #            target.acc_repayment_myr = source.acc_repayment_myr,
    #            target.acc_cumulative_repayment = source.acc_cumulative_repayment,
    #            target.acc_cumulative_repayment_myr = source.acc_cumulative_repayment_myr,


    cursor.execute("drop table A_DIS_N_REPAYMENT")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'Disbursement & Repayment';
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
    cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database Disbursement & Repayment",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY003', [jobErrDetail]= 'Update Database Disbursement & Repayment'
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Update Database Disbursement & Repayment Error: {e}")
    sys.exit(f"Update Database Disbursement & Repayment Error: {str(e)}")

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
######################################################

#appendfinal3

#file_name='02. Disbursement Repayment '+str(year)+"-"+str(month)+'.csv',