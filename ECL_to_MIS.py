#view dependency
#account need unique

# python ECL_to_MIS.py 9,"ECL 1024 - MIS v1.xlsx","ECL to MIS","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python ECL_to_MIS.py 9 "ECL 1024 - MIS v1.xlsx" "ECL to MIS" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-03-29"
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
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='ECL_to_MIS.py', [jobCompleted] = NULL
    WHERE [jobName] = 'ECL to MIS';
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
    #documentName = "a"
    #uploadedByEmail = "a"

    df1 =  os.path.join(config.FOLDER_CONFIG["FTP_directory"],documentName) #"ECL 1024 - MIS v1.xlsx" #documentName

    D1 = "LAF (2)"
    D2 = "C&C (2)"

    LAF = pd.read_excel(df1, sheet_name=D1, header=2)
    CnC = pd.read_excel(df1, sheet_name=D2, header=2)
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
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","ECL to MIS",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Upload Excel ECL to MIS'
    WHERE [jobName] = 'ECL to MIS';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Upload Excel ECL to MIS Error: {e}")
    sys.exit(f"Upload Excel ECL to MIS Error: {str(e)}")
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
    LAF.columns = LAF.columns.str.replace("\n", "_")
    LAF.columns = LAF.columns.str.replace(" ", "_")
    LAF.columns = LAF.columns.str.replace(".", "_")

    CnC.columns = CnC.columns.str.replace("\n", "_")
    CnC.columns = CnC.columns.str.replace(" ", "_")
    CnC.columns = CnC.columns.str.replace(".", "_")

    LAF1 = LAF.iloc[np.where(~(LAF.Account_No.isna()))]
    LAF1['LAF_ECL_MYR'] = LAF1['Stage_1_Conventional'] + LAF1['Stage_2_Conventional'] + LAF1['Stage_1_Islamic'] + LAF1['Stage_2_Islamic']
    LAF1['Account_No'] = LAF1['Account_No'].astype(str)
    LAF1 = LAF1.fillna(0).groupby(['Account_No','Borrower_name','Category','Unnamed:_5'])[['LAF_ECL_MYR']].sum().reset_index()
    LAF1['Unnamed:_5'] = LAF1['Unnamed:_5'].astype(str)
    #LAF1['Unnamed:_5'] = LAF1['Unnamed:_5'].str.strip()
    LAF1.rename(columns={'Account_No':'Account_No'},inplace=True)
    LAF1['Account_No'] = LAF1['Account_No'].astype(str)

    #================================================================================================
    Currency = pd.read_sql_query("""Select finance_sap_number
    ,b.param_name as currency
    from col_facilities_application_master a
    left outer join param_system_param b on a.facility_ccy_id = b.param_id;""", conn)

    Currency['finance_sap_number'] = Currency['finance_sap_number'].astype(str)
    Currency.columns = Currency.columns.str.replace("\n", "")

    LAF1_1 = LAF1.merge(Currency.drop_duplicates('finance_sap_number',keep='first').rename(columns={'finance_sap_number':'Account_No'}),on=['Account_No'],how='left', suffixes=('_x', '')) #,indicator=True

    #LAF1_1._merge.value_counts()
    #LAF1_1.iloc[np.where(LAF1_1['_merge']=='left_only')]
    #Currency.iloc[np.where(Currency.finance_sap_number==501086)]


    #view
    #sql ="select * from vw_GetLatestCurrencyRate"
    #MRate = pd.read_sql_query(sql, conn)
    aa = pd.read_sql_query("""SELECT param_name,r.exchange_rate,r.valuedate
    FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
    order by param_name asc;""", conn)
    
    MRate1 = aa.iloc[np.where(aa.valuedate==reportingDate)]

    df_add = pd.DataFrame([['MYR',
                        '1',
                        reportingDate]], columns=['param_name','exchange_rate','valuedate'])

    MRate = pd.concat([MRate1, df_add])

    MRate['exchange_rate'] = MRate['exchange_rate'].astype(float)

    #====================================================================================================================================
    #import pandas as pd
    #import pyodbc
    #import config
    #from datetime import datetime

    #reportingDate = '2024-03-29'
    #date_obj = datetime.strptime(reportingDate, "%Y-%m-%d")
    
    #conn = pyodbc.connect(config.CONNECTION_STRING)
    #cursor = conn.cursor()

    #aa = pd.read_sql_query("""SELECT param_name,r.exchange_rate,r.valuedate
    #  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
    #  order by param_name asc;""", conn)
    
    #import numpy as np
    #aa1 = aa.iloc[np.where(aa.valuedate==reportingDate)]
    #====================================================================================================================================

    #Rate
    LAF1_1['currency'] = LAF1_1['currency'].astype(str)
    LAF2 = LAF1_1.rename(columns={'currency':'param_name'}).merge(MRate[['param_name','exchange_rate','valuedate']], on='param_name', how='left') #,indicator=True

    LAF2['LAF_ECL_FC'] = LAF2['LAF_ECL_MYR']/LAF2['exchange_rate']

    #================================================================================================

    CnC['Account_No'] = CnC['Account_No'].astype(str)
    CnC1 = CnC.iloc[np.where(~(CnC.Account_No.isna()))]
    CnC1['CnC_ECL_MYR'] = CnC1['Stage_1_Conventional'] + CnC1['Stage_2_Conventional'] + CnC1['Stage_1_Islamic'] + CnC1['Stage_2_Islamic']
    CnC1 = CnC1.fillna(0).groupby(['Account_No','Borrower_name','Category','Unnamed:_5'])[['CnC_ECL_MYR']].sum().reset_index()
    #CnC1['Account_No'] = CnC1['Account_No'].str.strip()

    CnC1_1 = CnC1.merge(Currency.drop_duplicates('finance_sap_number',keep='first').rename(columns={'finance_sap_number':'Account_No'}),on=['Account_No'],how='left', suffixes=('_x', '')) #,indicator=True

    #with x as (SELECT max(r.valuedate) as le
    #  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
    #  where valuedate=valuedate)
    
    #SELECT param_name,r.exchange_rate,r.valuedate
    #  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
    #  inner join x on x.le = valuedate order by param_name asc

    CnC2 = CnC1_1.rename(columns={'currency':'param_name'}).merge(MRate[['param_name','exchange_rate','valuedate']], on='param_name', how='left')
    CnC2['CnC_ECL_FC'] = CnC2['CnC_ECL_MYR']/CnC2['exchange_rate']

    #================================================================================================

    merge = pd.concat([LAF2,CnC2])
    merge.fillna(0, inplace=True)

    #merge.Borrower_name = merge.Borrower_name.str.strip()
    merge.Borrower_name = merge.Borrower_name.str.upper()

    merge1 = merge.iloc[np.where(merge['Account_No']!="nan")].fillna(0).groupby(['Account_No'])[['LAF_ECL_FC',
                                                            'LAF_ECL_MYR',
                                                            'CnC_ECL_FC',
                                                            'CnC_ECL_MYR']].sum().reset_index()
    
    merge1['ECL_FC'] = merge1['LAF_ECL_FC'].fillna(0) + merge1['CnC_ECL_FC'].fillna(0)
    merge1['ECL_MYR'] = merge1['LAF_ECL_MYR'].fillna(0) + merge1['CnC_ECL_MYR'].fillna(0)

    df_add_Humm = pd.DataFrame([['500776A',
                        0,
                        0,
                        0,
                        0,
                        0,
                        0]], columns=['Account_No',
                                              'LAF_ECL_FC',
                                              'LAF_ECL_MYR',
                                              'CnC_ECL_FC',
                                              'CnC_ECL_MYR',
                                              'ECL_FC',
                                              'ECL_MYR'])

    merge1 = pd.concat([merge1, df_add_Humm])

    a_humm = sum(merge1.fillna(0).iloc[np.where(merge1['Account_No']=='500776')]['LAF_ECL_FC'])
    b_humm = sum(merge1.fillna(0).iloc[np.where(merge1['Account_No']=='500776')]['LAF_ECL_MYR'])
    c_humm = sum(merge1.fillna(0).iloc[np.where(merge1['Account_No']=='500776')]['CnC_ECL_FC'])
    d_humm = sum(merge1.fillna(0).iloc[np.where(merge1['Account_No']=='500776')]['CnC_ECL_MYR'])
    e_humm = sum(merge1.fillna(0).iloc[np.where(merge1['Account_No']=='500776')]['ECL_FC'])
    f_humm = sum(merge1.fillna(0).iloc[np.where(merge1['Account_No']=='500776')]['ECL_MYR'])

    merge1.loc[(merge1['Account_No']=='500776'),'LAF_ECL_FC'] = 0.79*a_humm
    merge1.loc[(merge1['Account_No']=='500776'),'LAF_ECL_MYR'] = 0.79*b_humm
    merge1.loc[(merge1['Account_No']=='500776'),'CnC_ECL_FC'] = 0.79*c_humm
    merge1.loc[(merge1['Account_No']=='500776'),'CnC_ECL_MYR'] = 0.79*d_humm
    merge1.loc[(merge1['Account_No']=='500776'),'ECL_FC'] = 0.79*e_humm
    merge1.loc[(merge1['Account_No']=='500776'),'ECL_MYR'] = 0.79*f_humm
    
    merge1.loc[(merge1['Account_No']=='500776A'),'LAF_ECL_FC'] = 0.21*a_humm
    merge1.loc[(merge1['Account_No']=='500776A'),'LAF_ECL_MYR'] = 0.21*b_humm
    merge1.loc[(merge1['Account_No']=='500776A'),'CnC_ECL_FC'] = 0.21*c_humm
    merge1.loc[(merge1['Account_No']=='500776A'),'CnC_ECL_MYR'] = 0.21*d_humm
    merge1.loc[(merge1['Account_No']=='500776A'),'ECL_FC'] = 0.21*e_humm
    merge1.loc[(merge1['Account_No']=='500776A'),'ECL_MYR'] = 0.21*f_humm

    convert_time = str(current_time).replace(":","-")
    merge1['position_as_at'] = reportingDate
    merge1.to_excel(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_ECL_to_MIS_"+str(convert_time)[:19]+".xlsx"),index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel ECL to MIS",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Process Excel ECL to MIS'
    WHERE [jobName] = 'ECL to MIS';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Process Excel ECL to MIS Error: {e}")
    sys.exit(f"Process Excel ECL to MIS Error: {str(e)}")

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

#--------------------------------------------------------connect ngan database-----------------------------------------------------------------------------------------------------------------------------------------------------

# cntrl + K + C untuk comment kn sume 
# cntrl + K + U untuk comment kn sume 

try:
    #table        
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Result_ECL_to_MIS_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')]
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

    # Assuming 'combine2' is a DataFrame
    column_types = []
    for col in merge1.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if merge1[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif merge1[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif merge1[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others


    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_ECL_TO_MIS (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in merge1.iterrows():
        sql = "INSERT INTO A_ECL_TO_MIS({}) VALUES ({})".format(','.join(merge1.columns), ','.join(['?']*len(merge1.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_ECL_TO_MIS AS source
    ON target.finance_sap_number = source.Account_No
    WHEN MATCHED THEN
        UPDATE SET target.acc_credit_loss_laf_ecl = source.LAF_ECL_FC,
                target.acc_credit_loss_laf_ecl_myr = source.LAF_ECL_MYR,
                target.acc_credit_loss_cnc_ecl = source.CnC_ECL_FC,
                target.acc_credit_loss_cnc_ecl_myr = source.CnC_ECL_MYR,
                target.acc_credit_loss_lafcnc_ecl = source.ECL_FC,
                target.acc_credit_loss_lafcnc_ecl_myr = source.ECL_MYR,
                target.position_as_at = source.position_as_at;
    """)
    conn.commit() 
    cursor.execute("drop table A_ECL_TO_MIS")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'ECL to MIS';
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
    cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database ECL to MIS",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Update Database ECL to MIS'
    WHERE [jobName] = 'ECL to MIS';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Update Database ECL to MIS Error: {e}")
    sys.exit(f"Update Database ECL to MIS Error: {str(e)}")

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
#---------------------------------------------Download-------------------------------------------------------------

#test sql

#with x as (SELECT max(r.valuedate) as le
#  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
#  where valuedate=valuedate)
 
#SELECT param_name,r.exchange_rate,r.valuedate
#  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
#  inner join x on x.le = valuedate order by param_name asc

#-- eomonth(valuedate)


#with x as (SELECT max(r.valuedate) as le
#  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
#  where month(valuedate)=month(getdate())-1 )

#SELECT param_name,r.exchange_rate,r.valuedate
#  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
#  where year(valuedate)=year(GETDATE()) order by valuedate desc
