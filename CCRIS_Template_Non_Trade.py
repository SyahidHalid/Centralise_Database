# python CCRIS_Template.py 1, "a", "CCRIS Template", "Pending Processing", "0", "syahidhalid@exim.com.my","2025-07-31"

#   reportingDate = '2026-05-31' #UAT mis_db_prod_11062026
#   documentId = 1

#   Library
import os
import sys
import pyodbc
import config
import pandas as pd
import numpy as np
import datetime as dt

#   Display
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


#   Library
try:
    import pandas as pd
    import numpy as np
    import pyodbc
    import datetime as dt
    import xlsxwriter

    pd.set_option("display.max_columns", None) 
    pd.set_option("display.max_colwidth", 1000) #huruf dlm column
    pd.set_option("display.max_rows", 100)
    pd.set_option("display.precision", 2) #2 titik perpuluhan

except Exception as e:
    print(f"Library Error: {e}")
    sys.exit(f"Library Error: {str(e)}")
    #sys.exit(1)
        
#----------------------------------------------------------------------------------------------------


#   pyodbc
try:
    #conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"+
    #                    "Server=10.32.1.51,1455;"+
    #                    "Database=mis_db_prod_backup_2024_04_02;"+
    #                    "Trusted_Connection=no;"+
    #                    "uid=mis_admin;"+
    #                    "pwd=Exim1234")
    conn = pyodbc.connect(config.CONNECTION_STRING)
    
    cursor = conn.cursor()


    Active_before = pd.read_sql_query(
        "SELECT * FROM dbase_account_hist WHERE position_as_at = ?",
        conn,
        params=(reportingDate,)
    )

    sql_query1 = """UPDATE [jobPython]
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='CCRIS_Template.py',[jobCompleted] = NULL
    WHERE [jobName] = 'CCRIS Template';
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
    #   Active_before.iloc[np.where(Active_before['finance_sap_number']=='501058')][['int_month_in_arrears','installment_in_arrears']]
    #   Active_before['int_month_in_arrears'].value_counts()

    Active_before.loc[Active_before['int_month_in_arrears']!=0, 'installment_in_arrears'] = Active_before['int_month_in_arrears'] + 1

    Active_before1 = Active_before[['cif_name',
                                    'finance_sap_number',
                                    'facility_application_sys_code',
                                    'facility_ccris_master_account_num',
                                    'facility_ccris_master_account_num',
                                    'position_as_at',
                                    'acc_principal_amount_outstanding',
                                    'acc_accrued_interest_myr',
                                    'acc_other_charges_myr',
                                    'total_loans_outstanding_myr',
                                    'int_month_in_arrears'
]]

    #   Active_before.head(1)
    #   Active_before.shape
    def format_18_digit(val: str) -> str:
        val = str(val)
        if len(val) == 18 and val.isdigit():
            return f'{val[0:4]}-{val[4:9]}-{val[9:12]}-{val[12:16]}-{val[16:18]}'
        return val

    Active_before['No.'] = range(1, len(Active_before) + 1)

    






    #---------------------------------------------Details-------------------------------------------------------------
    
    # Extract
    # LDB4.head(1)
    # LDB4.shape
    convert_time = str(current_time).replace(":","-")
    #Loan Database
    writer2 = pd.ExcelWriter(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"CCRIS_Template_"+str(convert_time)[:19]+".xlsx"),engine='xlsxwriter')

    LDB4.to_excel(writer2, sheet_name='loandatabase', index = False, startrow=2)

    writer2.close()

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'CCRIS Template';
                """
    cursor.execute(sql_query4)
    conn.commit() 

    #table    
    # documentId = 1    
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"CCRIS_Template_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')] #cari pakai code jgn pakai id ,36978,36960
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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel CCRIS Template",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Process Excel CCRIS Template'
    WHERE [jobName] = 'CCRIS Template';
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

    print(f"Process Excel CCRIS TemplateError: {e}")
    sys.exit(f"Process Excel CCRIS Template Error: {str(e)}")


#  select [Master Account Number] as [CCRIS Master Account Number]
# ,[Sub Account Number] as [CCRIS Sub Account Number]
# ,[Amount Disbursed_During the Month (RM)] as [Disbursement/Drawdown (MYR)]
# ,[Months in arrears] as [Month in Arrears]
# , [Principal Outstanding_(RM)]  as [Cost/Principal Outstanding (MYR)]
# ,[Interest / Income Outstanding (RM)] as [Cumulative Accrued Profit/Interest (MYR)]
# , 0 as [Income/Interest in Suspense (MYR)]
# ,[Classification of Exposures] as [MFRS9 Staging]
# , 0 as [Expected Credit Loss LAF (ECL) (MYR)]
# ,[Late Payment Charges for Ta'widh (Compensation) During the Month] as [Penalty/Ta'widh (MYR)]
# ,[Other Charges (RM)] as [Other Charges (MYR)] 
# ,[Amount Undrawn (RM)]  as [Unutilised/_Undrawn Amount (MYR)]
# ,[Amount Disbursed_During the Month (RM)]
# , [Amount Repaid During the Month (RM)]
# ,[Sub Account Number]  as [Finance(SAP) Number]
# ,[Probability of Default (%)] as [PD (%)]
# ,[Loss Given Default (%)] as [LGD (%)]
# from [non-trade$A8:AG] where [no] is not null and [customer Number] = 0