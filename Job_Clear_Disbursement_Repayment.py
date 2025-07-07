# python Job_Clear_Disbursement_Repayment.py 9 "Disbursement&RepaymentMay2025.xlsx.xlsx.xlsx.xlsx" "Job Clear Disbursement Repayment" "Pending Processing" "0" "syahidhalid@exim.com.my" "2025-05-31"

# documentId = 100
# documentName = "Disbursement&RepaymentMay2025.xlsx.xlsx.xlsx.xlsx"
# jobName = "Job Clear Disbursement Repayment"
# statusName = "Pending Processing"
# uploadedById = "0"
# uploadedByEmail = "syahidhalid@exim.com.my"
# reportingDate = "2025-05-31"

# Library & DB
import os
import sys
import pyodbc
import config
import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

#   Timestamp
current_time = pd.Timestamp.now()
convert_time = str(current_time).replace(":","-")
#print("Arguments passed:", sys.argv)


# Database connection setup
def connect_to_mssql():
    try:
        connection = pyodbc.connect(config.CONNECTION_STRING)
        print("Connected to MSSQL database successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to MSSQL database: {e}")
        sys.exit(f"Error connecting to MSSQL database: {str(e)}")

#______________________________________________________________________________________________________________________________________________________________________________________________________________________________________

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

#______________________________________________________________________________________________________________________________________________________________________________________________________________________________________

#   pyodbc
try:
    conn = pyodbc.connect(config.CONNECTION_STRING)

    cursor = conn.cursor()

    LDB_prev = pd.read_sql_query("SELECT * FROM col_facilities_application_master;", conn)

    sql_query1 = """UPDATE [jobPython]
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Job_Clear_Disbursement_Repayment.py', [jobCompleted] = NULL
    WHERE [jobName] = 'Job Clear Disbursement Repayment';
                """
    cursor.execute(sql_query1)
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")
    sys.exit(f"Connect to Database Error: {str(e)}")

#______________________________________________________________________________________________________________________________________________________________________________________________________________________________________

#   process
try:
    # documentName = "ECLS1S2May-2025working10.6.258.07pm.xlsx.xlsx"
    # reportingDate = "2025-05-31"

    LDB_prev1 = LDB_prev[['facility_exim_account_num',
                          'acc_drawdown_fc',
                          'acc_drawdown_myr',
                          'acc_cumulative_drawdown',
                          'acc_cumulative_drawdown_myr',
                          'acc_repayment_fc',
                          'acc_repayment_myr',
                          'acc_cumulative_repayment',
                          'acc_cumulative_repayment_myr']]#.fillna(0)


    LDB_prev1['acc_cumulative_drawdown'] = LDB_prev1['acc_cumulative_drawdown'].fillna(0) - LDB_prev1['acc_drawdown_fc'].fillna(0)
    LDB_prev1['acc_cumulative_drawdown_myr'] = LDB_prev1['acc_cumulative_drawdown_myr'].fillna(0) - LDB_prev1['acc_drawdown_myr'].fillna(0)
    LDB_prev1['acc_cumulative_repayment'] = LDB_prev1['acc_cumulative_repayment'].fillna(0) - LDB_prev1['acc_repayment_fc'].fillna(0)
    LDB_prev1['acc_cumulative_repayment_myr'] = LDB_prev1['acc_cumulative_repayment_myr'].fillna(0) - LDB_prev1['acc_repayment_myr'].fillna(0)

    LDB_prev1['acc_drawdown_fc'] = 0.00
    LDB_prev1['acc_drawdown_myr'] = 0.00
    LDB_prev1['acc_repayment_fc'] = 0.00
    LDB_prev1['acc_repayment_myr'] = 0.00

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
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","Disbursement Repayment",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY003', [jobErrDetail]= 'Upload Job Clear Disbursement Repayment'
    WHERE [jobName] = 'Job Clear Disbursement Repayment';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Upload Job Clear Disbursement Repayment Error: {e}")
    sys.exit(f"Upload Job Clear Disbursement Repayment Error: {str(e)}")


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

#______________________________________________________________________________________________________________________________________________________________________________________________________________________________________

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

    create_table_query_result = "CREATE TABLE A_download_result_D (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_result.iterrows():
        sql_result = "INSERT INTO A_download_result_D({}) VALUES ({})".format(','.join(download_result.columns), ','.join(['?']*len(download_result.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()


    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_result_D AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);   
    """)
    conn.commit() 
    cursor.execute("drop table A_download_result_D")
    conn.commit() 

    #status id PY002
    #processed_status_id PY005

    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # LDB_prev1.facility_exim_account_num = LDB_prev1.facility_exim_account_num.str.replace("",0)
    # LDB_prev1.acc_drawdown_fc = LDB_prev1.acc_drawdown_fc.str.replace("",0)
    # LDB_prev1.acc_drawdown_myr = LDB_prev1.acc_drawdown_myr.str.replace("",0)
    # LDB_prev1.acc_cumulative_drawdown = LDB_prev1.acc_cumulative_drawdown.str.replace("",0)
    # LDB_prev1.acc_cumulative_drawdown_myr = LDB_prev1.acc_cumulative_drawdown_myr.str.replace("",0)
    # LDB_prev1.acc_repayment_fc = LDB_prev1.acc_repayment_fc.str.replace("",0)
    # LDB_prev1.acc_repayment_myr = LDB_prev1.acc_repayment_myr.str.replace("",0)
    # LDB_prev1.acc_cumulative_repayment = LDB_prev1.acc_cumulative_repayment.str.replace("",0)
    # LDB_prev1.acc_cumulative_repayment_myr = LDB_prev1.acc_cumulative_repayment_myr.str.replace("",0)

    column_types = []
    for col in LDB_prev1.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if LDB_prev1[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif LDB_prev1[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif LDB_prev1[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others


    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_DISBURSEMENT_REPAYMENT (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)


    for row in LDB_prev1.iterrows():
        sql = "INSERT INTO A_DISBURSEMENT_REPAYMENT({}) VALUES ({})".format(','.join(LDB_prev1.columns), ','.join(['?']*len(LDB_prev1.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()

    # LAF3["facility_exim_account_num"].value_counts()
    # LAF3.iloc[np.where(LAF3["facility_exim_account_num"]=="330801137110034000")]

    # cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_DISBURSEMENT_REPAYMENT AS source
    # ON target.facility_exim_account_num = source.facility_exim_account_num
    # WHEN MATCHED THEN
    #     UPDATE SET target.acc_drawdown_fc = source.acc_drawdown_fc,
    #             target.acc_drawdown_myr = source.acc_drawdown_myr,
    #             target.acc_cumulative_drawdown = source.acc_cumulative_drawdown,
    #             target.acc_cumulative_drawdown_myr = source.acc_cumulative_drawdown_myr,
    #             target.acc_repayment_fc = source.acc_repayment_fc,
    #             target.acc_repayment_myr = source.acc_repayment_myr,
    #             target.acc_cumulative_repayment = source.acc_cumulative_repayment,
    #             target.acc_cumulative_repayment_myr = source.acc_cumulative_repayment_myr,

    # """, (reportingDate,))
    # conn.commit()
    #target.position_as_at = ? AND
    
    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_DIS_N_REPAYMENT AS source
    ON target.finance_sap_number = source.Account
    WHEN MATCHED AND target.position_as_at = ? THEN
        UPDATE SET target.acc_drawdown_myr = source.acc_drawdown_myr,
                target.acc_cumulative_drawdown_myr = source.acc_cumulative_drawdown_myr,
                target.acc_repayment_myr = source.acc_repayment_myr,
                target.acc_cumulative_repayment_myr = source.acc_cumulative_repayment_myr;
    """, (reportingDate,))
    conn.commit() 

    cursor.execute("drop table A_DISBURSEMENT_REPAYMENT")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'Job Clear Disbursement Repayment';
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
    cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database Job Clear Disbursement Repayment",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY003', [jobErrDetail]= 'Update Job Clear Database Disbursement Repayment'
    WHERE [jobName] = 'Job Clear Disbursement Repayment';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Update Database Job Clear Disbursement Repayment Error: {e}")
    sys.exit(f"Update Database Job Clear Disbursement Repayment Error: {str(e)}")

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

#______________________________________________________________________________________________________________________________________________________________________________________________________________________________________
    