# python Final_Disbursement_Repayment.py "0","Disbursement & Repayment October 2024.xlsx","Disbursement & Repayment","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python Final_Disbursement_Repayment.py "0" "Disbursement & Repayment October 2024.xlsx" "Disbursement & Repayment" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-03-29"
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
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Disbursement_Repayment.py', [jobCompleted] = NULL
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_query1)
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")

#------------------------------------------------------------------------------------------------

#process
try:
    #    #E:\PythonProjects\misPython\misPython_doc
    #df1 = documentName #"Disbursement & Repayment October 2024.xlsx" #
    df1 =  config.FOLDER_CONFIG["FTP_directory"]+documentName #"ECL 1024 - MIS v1.xlsx" #documentName

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
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Upload Excel Disbursement & Repayment'
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_error)
    conn.commit() 

 #---------------------------------Start

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
    
    appendfinal3.to_excel(config.FOLDER_CONFIG["FTP_directory"]+"Result_Disbursement_Repayment_"+str(convert_time)[:19]+".xlsx",index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

    #appendfinal3.to_excel("Disbursement_Repayment_"+str(convert_time)[:19]+".xlsx",index=False)
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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel Disbursement & Repayment",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= '"Process Excel Disbursement & Repayment'
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_error)
    conn.commit() 



#---------------------------------------------Database-------------------------------------------------------------

try:
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


    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_DIS_N_REPAYMENT (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in appendfinal3.iterrows():
        sql = "INSERT INTO A_DIS_N_REPAYMENT({}) VALUES ({})".format(','.join(appendfinal3.columns), ','.join(['?']*len(appendfinal3.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()

    cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_DIS_N_REPAYMENT AS source
    ON target.finance_sap_number = source.Account
    WHEN MATCHED THEN
        UPDATE SET target.acc_drawdown_fc = source.acc_drawdown_fc,
                target.acc_drawdown_myr = source.acc_drawdown_myr,
                target.acc_cumulative_drawdown = source.acc_cumulative_drawdown,
                target.acc_cumulative_drawdown_myr = source.acc_cumulative_drawdown_myr,
                target.acc_repayment_fc = source.acc_repayment_fc,
                target.acc_repayment_myr = source.acc_repayment_myr,
                target.acc_cumulative_repayment = source.acc_cumulative_repayment,
                target.acc_cumulative_repayment_myr = source.acc_cumulative_repayment_myr,
                target.position_as_at = source.position_as_at;
    """)
    conn.commit() 

    cursor.execute("drop table A_DIS_N_REPAYMENT")
    conn.commit() 

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002'
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
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Update Database Disbursement & Repayment'
    WHERE [jobName] = 'Disbursement & Repayment';
                """
    cursor.execute(sql_error)
    conn.commit() 

######################################################

#appendfinal3

#file_name='02. Disbursement Repayment '+str(year)+"-"+str(month)+'.csv',