#view dependency
#account need unique

# python PD_LGD.py 8,"11. ECL Computation Client Template Nov-24 (Regular) - revised.xlsm","PD LGD","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python PD_LGD.py 8 "11. ECL Computation Client Template Nov-24 (Regular) - revised.xlsm" "PD LGD" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-03-29"
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
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='PD_LGD.py', [jobCompleted] = NULL
    WHERE [jobName] = 'PD LGD';
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
    # import config
    # import pandas as pd
    # import numpy as np
    # import pyodbc
    # from datetime import datetime
    # import config

    # pd.set_option("display.max_columns", None) 
    # pd.set_option("display.max_colwidth", 1000) #huruf dlm column
    # pd.set_option("display.max_rows", 100)
    # pd.set_option("display.precision", 2) #2 titik perpuluhan

    # #   Timestamp
    # current_time = pd.Timestamp.now()

    # reportingDate = "2024-12-30"

    # df1 =  str(config.FOLDER_CONFIG["FTP_directory"])+"11. ECL Computation Client Template Nov-24 (Regular) - revised.xlsm" #"ECL 1024 - MIS v1.xlsx" #documentName
    df1 =  os.path.join(config.FOLDER_CONFIG["FTP_directory"],documentName)

    D1 = "Active"
    D2 = "FL PD"

    Active = pd.read_excel(df1, sheet_name=D1, header=6)
    Lifetime = pd.read_excel(df1, sheet_name=D2, header=7, usecols="B:V", nrows=50)
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
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","PD LGD",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Upload Excel PD LGD'
    WHERE [jobName] = 'PD LGD';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Upload Excel PD LGD Error: {e}")
    sys.exit(f"Upload Excel PD LGD Error: {str(e)}")
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

#----------------------------------------------------------------------------------------------------

try:
    Active1 = Active.iloc[np.where(~Active['Finance (SAP) Number'].isna())]
    Lifetime1 = Lifetime#.drop(['Unnamed: 0'],axis=1)

    Active1["PD segment"] = Active1["PD segment"].str.upper()
    Lifetime1["PD/Year"] = Lifetime1["PD/Year"].str.upper()

    Active1["Reporting date"] = pd.to_datetime(reportingDate, utc=True)
    Active1["Maturity date"] = pd.to_datetime(Active1["Maturity date"], utc=True)
    Active1["YOB"] = ((Active1["Maturity date"].dt.year - Active1["Reporting date"].dt.year)*12+(Active1["Maturity date"].dt.month - Active1["Reporting date"].dt.month))+1

    Active2 = Active1.merge(Lifetime1.rename(columns={'PD/Year':'PD segment'}), on="PD segment",how='left')

    Active2['PD (%)'] = Active2.loc[Active2["PD segment"]==Active2["PD segment"], 1] 
    #Active2.iloc[np.where(Active2['YOB']<=24)]

    Active3 = Active2[['Finance (SAP) Number',
                    'Borrower name',
                    'Reporting date',
                    'Maturity date',
                    'YOB',
                    'PD segment',
                    'PD (%)',
                    'LGD Segment',
                    'LGD rate',
                    'Watchlist (Yes/No)']].rename(columns={'Finance (SAP) Number':'finance_sap_number',
                                                'Borrower name':'Borrower',
                                                'Reporting date':'Reporting_Date',
                                                'Maturity date':'Maturity_Date',
                                                'PD segment':'PD_Segment','LGD Segment':'LGD_Segment',
                                                 'PD (%)':'acc_pd_percent',
                                                 'LGD rate':'acc_lgd_percent',
                                                 'Watchlist (Yes/No)':'Watchlist_Tagging'}).fillna(0)
    
    Active3['finance_sap_number'] =Active3['finance_sap_number'].astype(str)

    Active3['Reporting_Date'] =Active3['Reporting_Date'].astype(str)
    Active3['Maturity_Date'] = Active3['Maturity_Date'].astype(str)
    
    #Active3['Reporting_Date'] = str(Active3['Reporting_Date'])[:19]
    #Active3['Maturity_Date'] = str(Active3['Maturity_Date'])[:19]

    convert_time = str(current_time).replace(":","-")
    Active3['position_as_at'] = reportingDate
    #Active3.to_excel("Result_PD_LGD_.xlsx",index=False)

    Active4 = Active3[['finance_sap_number',
    'Borrower',
    'Reporting_Date',
    'Maturity_Date',
    'YOB',
    'PD_Segment',
    'acc_pd_percent','LGD_Segment','acc_lgd_percent','position_as_at','Watchlist_Tagging']]

    #Active4.to_excel(config.FOLDER_CONFIG["FTP_directory"]+"Result_PD_LGD.xlsx",index=False) #"ECL 1024 - MIS v1.xlsx" #documentName
    #Active4.to_excel(config.FOLDER_CONFIG["FTP_directory"]+"Result_PD_LGD_"+str(convert_time)[:19]+".xlsx",index=False) #"ECL 1024 - MIS v1.xlsx" #documentName
    Active4.to_excel(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_PD_LGD_"+str(convert_time)[:19]+".xlsx"),index=False) #"ECL 1024 - MIS v1.xlsx" #documentName
    
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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel PD LGD",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Process Excel PD LGD'
    WHERE [jobName] = 'PD LGD';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Process Excel PD LGD Error: {e}")
    sys.exit(f"Process Excel PD LGD Error: {str(e)}")
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

try:
    #table        
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Result_PD_LGD_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')]
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

    create_table_query_result = "CREATE TABLE A_download_result_E (" + ', '.join(column_types1) + ")"
    cursor.execute(create_table_query_result)

    for row in download_result.iterrows():
        sql_result = "INSERT INTO A_download_result_E({}) VALUES ({})".format(','.join(download_result.columns), ','.join(['?']*len(download_result.columns)))
        cursor.execute(sql_result, tuple(row[1]))
    conn.commit()


    cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
                    USING A_download_result_E AS source
                    ON target.aftd_id = source.aftd_id
                    WHEN MATCHED THEN 
                        UPDATE SET target.result_file_name = source.result_file_name,
                        target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
                        target.status_id = (select param_id from param_system_param where param_code=source.status_id);    
    """)
    conn.commit() 
    cursor.execute("drop table A_download_result_E")
    conn.commit() 

    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # Assuming 'combine2' is a DataFrame
    column_types = []
    for col in Active4.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if Active4[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif Active4[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif Active4[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others

    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_PD_LGD (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in Active4.iterrows():
        sql = "INSERT INTO A_PD_LGD({}) VALUES ({})".format(','.join(Active4.columns), ','.join(['?']*len(Active4.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_PD_LGD AS source
    ON target.finance_sap_number = source.finance_sap_number
    WHEN MATCHED THEN
        UPDATE SET target.acc_pd_percent = source.acc_pd_percent,
                target.acc_lgd_percent = source.acc_lgd_percent,
                target.position_as_at = source.position_as_at;
    """)
    conn.commit() 
    cursor.execute("drop table A_PD_LGD")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'PD LGD';
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
    cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database PD LGD",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Update Database PD LGD'
    WHERE [jobName] = 'PD LGD';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Update Database PD LGD Error: {e}")
    sys.exit(f"Update Database PD LGD Error: {str(e)}")

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

#SAP account, PD segment, maturity date, reporting date, month to mature, PD (%), LGD Segment, LGD (%)

#cntrl + k + c
#cntrl + k + u

# def pd_percen(YOB,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20):
#     if YOB<=12:
#         return asd
#     elif YOB>12 and YOB<=24:
#         return asd
#     elif YOB>24 and YOB<=36:
#         return asd
#     elif YOB>36 and YOB<=48:
#         return asd
#     elif YOB>48 and YOB<=60:
#         return asd
#     elif YOB>60 and YOB<=72:
#         return asd
#     elif YOB>72 and YOB<=84:
#         return asd
#     elif YOB>84 and YOB<=96:
#         return asd
#     elif YOB>96 and YOB<=108:
#         return asd
#     elif YOB>108 and YOB<=120:
#         return asd
#     elif YOB>120 and YOB<=132:
#         return asd
#     elif YOB>132 and YOB<=144:
#         return asd
#     elif YOB>144 and YOB<=156:
#         return asd
#     elif YOB>156 and YOB<=168:
#         return asd
#     elif YOB>168 and YOB<=180:
#         return asd
#     elif YOB>180 and YOB<=192:
#         return asd
#     elif YOB>192 and YOB<=204:
#         return asd
#     elif YOB>204 and YOB<=216:
#         return asd
#     elif YOB>216 and YOB<=228:
#         return asd
#     elif YOB>228 and YOB<=240:
#         return asd
#     else:
#         return 0
# Active2["PD (%)"] = Active2.apply(lambda x: pd_percen(x['']),axis=1)

#Active2["PD (%)"] = Active2.loc[Active2["PD segment"]==Active2["PD segment"], Active2.columns[55]] 

# Active2.head(1)
# Active2["YOB"][:1]
# value = Active2[Active2["PD segment"]=="L:PD6"][1][0]
# Active2["PD (%)"] = Active2.loc[Active2["PD segment"]==Active2["PD segment"],Active2.columns==Active2["YOB"]]
# Active2.columns[1]
# columncal = int(Active2["YOB"].dtypes)
# value = Active2.loc[Active2["PD segment"]=="L:PD6",12]
# #Active2.loc[Active2["PD segment"]=="L:PD6",12]
# Active1.shape
# Active2.shape
# Active2._merge.value_counts()
# Lifetime1[1]

# acc_pd_percent
# acc_lgd_percent

# =IF(D41="","",
#     IFERROR(INDEX('Lifetime PD'!$C$57:$ED$105,
#         MATCH(ECL!$D$18,
#             'Lifetime PD'!$B$57:$B$105,0),
#                 MATCH(ECL!D41,'Lifetime PD'!$C$56:$FZ$56,0)),0))

# D41 = 41 = YOB
# so reporting date -  maturity date

# SAP account, PD segment, maturity date, reporting date, month to mature, PD (%), LGD Segment, LGD (%)
# 501125, L:PD6, 30/11/2025, 30/11/2024, 12, 0.85%, Partially Secured, 39.5%


# import pandas as pd
# # Sample DataFrame
# data = {'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]}
# df = pd.DataFrame(data)
# value = df.loc[df['A'] == 2, 'B']
# print(value)  # Output: 5