# python ARRD_Upload.py 13, "ARRD-MIA.xlsx", "ARRD Upload", "Pending Processing", "0", "syahidhalid@exim.com.my","2025-12-31"

#   Library
import os
import sys
import pyodbc
import config
import pandas as pd
import numpy as np
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

print("Arguments passed:", sys.argv)

# Database connection setup
def connect_to_mssql():
    try:   
        #connection = pyodbc.connect(
        #    'DRIVER={ODBC Driver 17 for SQL Server};'
        #    'SERVER=10.32.1.51,1455;'
        #    'DATABASE=mis_db_prod_backup_2024_04_02;'
        #    'UID=mis_admin;'
        #    'PWD=Exim1234;'
        #    'Encrypt=yes;TrustServerCertificate=yes'  # Use if you encounter SSL issues
        #)

        connection = pyodbc.connect(config.CONNECTION_STRING)

        print("Connected to MSSQL database successfully.")
        
        return connection
    except Exception as e:
        print(f"Error connecting to MSSQL database: {e}")
        
        sys.exit(f"Error connecting to MSSQL database: {str(e)}")
        #sys.exit(1)

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
        #sys.exit(1)  # Exit the script with a failure code
    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
            print("Database connection closed.")
 
#----------------------------------------------------------------------------------------------------

try:
    #conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"+
    #                    "Server=10.32.1.51,1455;"+
    #                    "Database=mis_db_prod_backup_2024_04_02;"+
    #                    "Trusted_Connection=no;"+
    #                    "uid=mis_admin;"+
    #                    "pwd=Exim1234")
    #   pyodbc
    conn = pyodbc.connect(config.CONNECTION_STRING)
    
    cursor = conn.cursor()

    LDB_prev = pd.read_sql_query("SELECT * FROM col_facilities_application_master;", conn)
    
    sql_query1 = """UPDATE [jobPython]
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='ARRD_Upload.py',[jobCompleted] = NULL
    WHERE [jobName] = 'ARRD Upload';
                """
    cursor.execute(sql_query1)
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")
    sys.exit(f"Connect to Database Error: {str(e)}")
    #sys.exit(1)

#------------------------------------------------------------------------------------------------

#upload excel
try:
    #   Excel File

    # documentName = "ARRD-MIA.xlsx"
    # reportingDate = "2025-12-31"
    # df1 = r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\05. Interactive Dashboard\\Closing 202508\\Job Upload\\"+str(documentName) 

    df1 =  os.path.join(config.FOLDER_CONFIG["FTP_directory"],documentName) #"ECL 1024 - MIS v1.xlsx" #documentName
    
    D1 = "autoupload_MIS"

    ARRD = pd.read_excel(df1, sheet_name=D1, header=3)


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
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","Upload Excel ARRD Upload",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Upload Excel ARRD Upload'
    WHERE [jobName] = 'ARRD Upload';
                """
    cursor.execute(sql_error)
    conn.commit()

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

    print(f"Upload Excel Allowance Error: {e}")
    sys.exit(f"Upload Excel Allowance Error: {str(e)}")
#------------------------------------------------------------------------------------------------


#process
try:
    ARRD.columns = ARRD.columns.str.replace("\n", "_")
    ARRD.columns = ARRD.columns.str.replace(" ", "_")
    ARRD.columns = ARRD.columns.str.replace(".", "_")

    #ARRD.SAP_number.dtypes
    #ARRD.acc_effective_cost_borrowings.value_counts()

    ARRD1 = ARRD[['Customer_Name',
                  'SAP_number',
                  'Position_Date',
                  'Months_in_arrears',
                  'Effective_cost_of_borrowings',
                  'Interest_Margin',
                  'Profit/Interest_Rate',
                  'Penalty_Rate']].iloc[np.where((ARRD.SAP_number != 0) & (ARRD.SAP_number.astype(str).str.len() == 6))].rename(columns={'Months_in_arrears':'int_month_in_arrears',
                                                                                                                 'Effective_cost_of_borrowings':'acc_effective_cost_borrowings',
                                                                                                                 'Interest_Margin':'acc_margin',
                                                                                                                 'Profit/Interest_Rate':'acc_average_interest_rate',
                                                                                                                 'Penalty_Rate':'acc_tadwih_compensation'})

    
    ARRD1['acc_margin'] = ARRD1['acc_margin'].replace("-",0).astype(float)
    ARRD1['acc_average_interest_rate'] = ARRD1['acc_average_interest_rate'].replace("-",0).astype(float)
    ARRD1['acc_tadwih_compensation'] = ARRD1['acc_tadwih_compensation'].replace("-",0).astype(float)

    ARRD1['acc_effective_cost_borrowings'] = ARRD1['acc_effective_cost_borrowings'].str.upper()
    ARRD1['acc_effective_cost_borrowings'] = ARRD1['acc_effective_cost_borrowings'].str.replace(' ','')
    ARRD1.loc[ARRD1['acc_effective_cost_borrowings'] == 'FIX', 'acc_effective_cost_borrowings'] = 'FIXED'

    #   ARRD1.acc_effective_cost_borrowings.value_counts()
    #   ARRD1.iloc[np.where(ARRD1.acc_effective_cost_borrowings=='FIXED')]
    #   param_merge.iloc[np.where(param_merge.param_name=='FIXED')]

    
    param = pd.read_sql_query("SELECT * FROM param_system_param where parent_param_id = 31212;", conn)

    param_table = ARRD1[['acc_effective_cost_borrowings']].drop_duplicates().reset_index(drop=True)

    param_table['acc_effective_cost_borrowings'] = param_table['acc_effective_cost_borrowings'].str.upper()
    param_table['acc_effective_cost_borrowings'] = param_table['acc_effective_cost_borrowings'].str.replace(' ', '')

    param['param_name'] = param['param_name'].str.upper()
    
    param_merge = param_table.rename(columns={'acc_effective_cost_borrowings':'param_name'}).merge(param[['param_id','param_name']],on='param_name',how='left')


    ARRD2 = ARRD1.merge(param_merge.rename(columns={'param_name':'acc_effective_cost_borrowings'}),on='acc_effective_cost_borrowings',how='left')

    # ARRD2.param_id.dtypes
    # ARRD1.shape
    # LDB_prev.acc_effective_cost_borrowings.dtypes

    # ARRD2.param_id.value_counts()
    # param_merge.param_name.value_counts()
    
    ARRD2['param_id'] = ARRD2['param_id'].fillna(0).astype(int)    
    ARRD2['param_id'] = ARRD2['param_id'].astype(str)
    ARRD2.loc[ARRD2['param_id'] == '0', 'param_id'] = ''
    
    
    # ARRD2.iloc[np.where(ARRD2['param_id']==0)]
    #ARRD1.head(1)
    #ARRD1.shape
    #ARRD1.dtypes

    #Exception

    #LDB_name = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = ?;", conn, params=(reportingDate,))
    
    LDB_prev1 = LDB_prev[['finance_sap_number',
                          'int_month_in_arrears',
                          'acc_effective_cost_borrowings',
                          'acc_margin',
                          'acc_average_interest_rate',
                          'acc_tadwih_compensation']].iloc[np.where((LDB_prev.finance_sap_number.astype(str).str.len() == 6))]


    #   LDB_prev1.iloc[np.where(LDB_prev1['finance_sap_number'].str.contains('B'))]

    # ARRD1.shape
    # ARRD1.SAP_number.dtypes
    # LDB_prev1.finance_sap_number.dtypes

    LDB_prev1.finance_sap_number = LDB_prev1.finance_sap_number.astype(str)
    ARRD2.SAP_number = ARRD2.SAP_number.astype(str)

    exception_report = ARRD2.rename(columns={'SAP_number':'finance_sap_number'}).merge(LDB_prev1, on=['finance_sap_number'], how='outer', suffixes=('_Manual','_Mis'),indicator=True)

    #exception_report._merge.value_counts()
    # exception_report1.iloc[np.where(exception_report1.finance_sap_number=='500577')]

    exception_report["diff_MIA"] = exception_report["int_month_in_arrears_Manual"].fillna(0) - exception_report["int_month_in_arrears_Mis"].fillna(0)
    

    exception_report["diff_COST_BORROWING"] = np.where(
        exception_report["acc_effective_cost_borrowings_Manual"] == exception_report["acc_effective_cost_borrowings_Mis"],
        "Y",
        "N"
    )

    exception_report["diff_MARGIN"] = exception_report["acc_margin_Manual"].fillna(0) - exception_report["acc_margin_Mis"].fillna(0)
    exception_report["diff_INTEREST_RATE"] = exception_report["acc_average_interest_rate_Manual"].fillna(0) - exception_report["acc_average_interest_rate_Mis"].fillna(0)
    exception_report["diff_PENALTY_RATE"] = exception_report["acc_tadwih_compensation_Manual"].fillna(0) - exception_report["acc_tadwih_compensation_Mis"].fillna(0)

    convert_time = str(current_time).replace(":","-")
    exception_report.Position_Date.fillna(reportingDate,inplace=True)
    
    exception_report1 = exception_report[['Customer_Name',
                                            'finance_sap_number',
                                            'Position_Date',
                                            '_merge',
                                            'int_month_in_arrears_Manual',
                                            'int_month_in_arrears_Mis',
                                            'diff_MIA',
                                            'acc_effective_cost_borrowings_Manual',
                                            'acc_effective_cost_borrowings_Mis',
                                            'diff_COST_BORROWING',
                                            'acc_margin_Manual',
                                            'acc_margin_Mis',
                                            'diff_MARGIN',
                                            'acc_average_interest_rate_Manual',
                                            'acc_average_interest_rate_Mis',
                                            'diff_INTEREST_RATE',
                                            'acc_tadwih_compensation_Manual',
                                            'acc_tadwih_compensation_Mis',
                                            'diff_PENALTY_RATE']].rename(columns={'Position_Date':'position_as_at'})
    
    #LDB_prev1.finance_sap_number.value_counts()
    #LDB_prev.head(1)
    
    # Extract
    writer2 = pd.ExcelWriter(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_ARRD_Upload_"+str(convert_time)[:19]+".xlsx"),engine='xlsxwriter')

    ARRD2.to_excel(writer2, sheet_name='Result', index = False)

    exception_report1.to_excel(writer2, sheet_name='Exception', index = False)

    writer2.close()

    
    # cursor.execute("DROP TABLE IF EXISTS Exception_ARRD_Upload")
    # conn.commit()

    # # Assuming 'combine2' is a DataFrame
    # column_types1 = []
    # for col in exception_report1.columns:
    #     # You can choose to map column types based on data types in the DataFrame, for example:
    #     if exception_report1[col].dtype == 'object':  # String data type
    #         column_types1.append(f"{col} VARCHAR(255)")
    #     elif exception_report1[col].dtype == 'int64':  # Integer data type
    #         column_types1.append(f"{col} INT")
    #     elif exception_report1[col].dtype == 'float64':  # Float data type
    #         column_types1.append(f"{col} FLOAT")
    #     else:
    #         column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

    # #   exception_report1.dtypes
    # create_table_query_result = "CREATE TABLE Exception_ARRD_Upload (" + ', '.join(column_types1) + ")"
    # cursor.execute(create_table_query_result)

    # for row in exception_report1.iterrows():
    #     sql_result = "INSERT INTO Exception_ARRD_Upload({}) VALUES ({})".format(','.join(exception_report1.columns), ','.join(['?']*len(exception_report1.columns)))
    #     cursor.execute(sql_result, tuple(row[1]))
    # conn.commit()

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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel ARRD Upload",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Process Excel ARRD Upload'
    WHERE [jobName] = 'ARRD Upload';
                """
    cursor.execute(sql_error)
    conn.commit()


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

    print(f"Process Excel ARRD Upload Error: {e}")
    sys.exit(f"Process Excel ARRD Upload Error: {str(e)}")



try:
    # ARRD2.head()
    # ARRD3.head()
   
    # ARRD3.SAP_number.value_counts()
    # ARRD3.iloc[np.where(ARRD3.SAP_number==500577)]

    ARRD3 = ARRD2.rename(columns={'Position_Date':'position_as_at',
                                  'param_id':'acc_effective_cost_borrowings',
                                   'acc_effective_cost_borrowings':'name'}).drop(['Customer_Name','name'],axis=1).fillna(0)#.groupby(['SAP_number','position_as_at','acc_effective_cost_borrowings','name'])[['int_month_in_arrears',
                                #                                                                                                                'acc_margin',
                                #                                                                                                                'acc_average_interest_rate',
                                #                                                                                                                'acc_tadwih_compensation']].sum().reset_index()
    
    # sum(appendfinal1.LAF_ECL_MYR)

    # Assuming 'combine2' is a DataFrame
    column_types = []
    for col in ARRD3.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if ARRD3[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif ARRD3[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif ARRD3[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others

    cursor.execute("DROP TABLE IF EXISTS A_ARRD_Upload")
    conn.commit()

    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_ARRD_Upload (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in ARRD3.iterrows():
        sql = "INSERT INTO A_ARRD_Upload({}) VALUES ({})".format(','.join(ARRD3.columns), ','.join(['?']*len(ARRD3.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()
    
    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_ARRD_Upload AS source
    ON target.finance_sap_number = source.SAP_number
    WHEN MATCHED AND target.position_as_at = ? THEN
        UPDATE SET target.int_month_in_arrears = source.int_month_in_arrears,
                   target.acc_effective_cost_borrowings = source.acc_effective_cost_borrowings,
                   target.acc_margin = source.acc_margin,
                   target.acc_average_interest_rate = source.acc_average_interest_rate,
                   target.acc_tadwih_compensation = source.acc_tadwih_compensation;
    """, (reportingDate,))
    conn.commit() 

    cursor.execute("drop table A_ARRD_Upload")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'ARRD Upload';
                """
    cursor.execute(sql_query4)
    conn.commit() 

    #table    
    # documentId = 1    
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Result_ARRD_Upload_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')] #cari pakai code jgn pakai id ,36978,36960
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

    create_table_query_result = "CREATE TABLE A_download_result (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_result.iterrows():
        sql_result = "INSERT INTO A_download_result({}) VALUES ({})".format(','.join(download_result.columns), ','.join(['?']*len(download_result.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_result AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);    
    """)
    conn.commit() 

    cursor.execute("drop table A_download_result")
    conn.commit() 

    #target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id)
    #target.processed_status_id = source.processed_status_id

    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
    cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database ARRD Upload",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Update Database ARRD Upload'
    WHERE [jobName] = 'ARRD Upload';
                """
    cursor.execute(sql_error)
    conn.commit() 

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

    print(f"Update Database ARRD Upload Error: {e}")
    sys.exit(f"Update Database ARRD Upload Error: {str(e)}")
    
    #sys.exit(1)
    #except Exception as e:
    #    print(f"Python Error: {e}")
    #    sys.exit(f"Python Error: {str(e)}")
    #    sys.exit(1)

    # documentId = 1