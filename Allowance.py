# python Final_Allowance.py "0","Allowance_1024_Adjusted.xlsx","Allowance","Pending Processing","0","syahidhalid@exim.com.my","2024-03-29"
# python Final_Allowance.py "0" "Allowance_1024_Adjusted.xlsx" "Allowance" "Pending Processing" "0" "syahidhalid@exim.com.my" "2024-03-29"
# position_as_at
try:
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
            sys.exit(f"Error updating user: {str(e)}")
            sys.exit(1)

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
            sys.exit(f"Script failed with exception: {str(e)}")
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
        sys.exit(f"Library Error: {str(e)}")
        sys.exit(1)
        

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
        SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Allowance.py',[jobCompleted] = NULL
        WHERE [jobName] = 'Allowance';
                    """
        cursor.execute(sql_query1)
        conn.commit() 
    except Exception as e:
        print(f"Connect to Database Error: {e}")
        sys.exit(f"Connect to Database Error: {str(e)}")
        sys.exit(1)

    #------------------------------------------------------------------------------------------------

    #upload excel
    try:
        #   Excel File Name

        #E:mis_doc\\PythonProjects\\misPython\\misPython_doc
        #df1 = documentName #"Allowance_1024_Adjusted.xlsx"
        
        #import config
        #documentName = "Allowance_1024_Adjusted.xlsx"

        df1 =  str(config.FOLDER_CONFIG["FTP_directory"]+documentName) #"ECL 1024 - MIS v1.xlsx" #documentName
        #df1 = r"D:\\mis_doc\\PythonProjects\\misPython\\misPython_doc\\Allowance_1024_Adjusted.xlsx" #"Data Mirror October 2024.xlsx"

        D1 = "IA-Conv"
        D2 = "IA-Islamic"
        D3 = "IA-IIS"
        D4 = "IA C&C CONV"
        D5 = "IA C&C ISL"

        #May_RM = form.text_input("1st (IA) MYR Column Sequence Aug24 is 71, Add 1 for next: Sep24 72")
        #May_FC = form.text_input("1st (IA) FC Column Sequence Aug24 is 142, Add 2 for next: Sep24 is 144")

        #May_RM_Is = form.text_input("2nd (C&C) MYR Column Sequence Aug24 is 61, Add 1 for next: Sep24 62")
        #May_FC_Is = form.text_input("2nd (C&C) FC Column Sequence Aug24 is 122, Add 2 for next: Sep24 is 124")

        IA_Conv = pd.read_excel(df1, sheet_name=D1, header=6)
        IA_Isl = pd.read_excel(df1, sheet_name=D2, header=6)
        IA_IIS = pd.read_excel(df1, sheet_name=D3, header=6)
        CnC_Conv = pd.read_excel(df1, sheet_name=D4, header=6)
        CnC_Isl = pd.read_excel(df1, sheet_name=D5, header=6)
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
        cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","Upload Excel Allowance",uploadedByEmail))
        conn.commit()
        sql_error = """UPDATE [jobPython]
        SET [jobCompleted] = NULL, [jobErrDetail]= 'Upload Excel Allowance'
        WHERE [jobName] = 'Allowance';
                    """
        cursor.execute(sql_error)
        conn.commit()
        print(f"Upload Excel Allowance Error: {e}")
        sys.exit(f"Upload Excel Allowance Error: {str(e)}")
        sys.exit(1) 
        


        #---------------------------------------------------------------------------

    #process
    try:
        IA_Conv.columns = IA_Conv.columns.str.replace("\n", "_")
        IA_Conv.columns = IA_Conv.columns.str.replace(" ", "_")
        IA_Conv.columns = IA_Conv.columns.str.replace(".", "_")

        IA_Isl.columns = IA_Isl.columns.str.replace("\n", "_")
        IA_Isl.columns = IA_Isl.columns.str.replace(" ", "_")
        IA_Isl.columns = IA_Isl.columns.str.replace(".", "_")

        IA_IIS.columns = IA_IIS.columns.str.replace("\n", "_")
        IA_IIS.columns = IA_IIS.columns.str.replace(" ", "_")
        IA_IIS.columns = IA_IIS.columns.str.replace(".", "_")

        CnC_Conv.columns = CnC_Conv.columns.str.replace("\n", "_")
        CnC_Conv.columns = CnC_Conv.columns.str.replace(" ", "_")
        CnC_Conv.columns = CnC_Conv.columns.str.replace(".", "_")

        CnC_Isl.columns = CnC_Isl.columns.str.replace("\n", "_")
        CnC_Isl.columns = CnC_Isl.columns.str.replace(" ", "_")
        CnC_Isl.columns = CnC_Isl.columns.str.replace(".", "_")

        IA_Conv_1 = IA_Conv.iloc[np.where(~(IA_Conv.Loan_Acc_.isna())&~(IA_Conv.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA','Closing']].sum().reset_index()

        IA_Isl_1 = IA_Isl.iloc[np.where((~(IA_Isl.Loan_Acc_.isna()))&~(IA_Isl.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA','Closing']].sum().reset_index()

        IA_IIS.loc[IA_IIS.Borrower=="PT Mahakarya Inti Buana",'Loan_Acc_']='500039'
        
        IA_IIS['Loan_Acc_'].fillna(0, inplace=True)

        IA_IIS_1 = IA_IIS.iloc[np.where((~(IA_IIS.Loan_Acc_==0))&~(IA_IIS.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['IIS_(RM)','IIS_(FC)']].sum().reset_index()

        #IA_IIS_1['Loan_Acc_'] = IA_IIS_1['Loan_Acc_'].astype(int)

        CnC_Conv_1 = CnC_Conv.iloc[np.where((~(CnC_Conv.Loan_Acc_.isna()))&~(CnC_Conv.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA','Closing']].sum().reset_index()

        CnC_Isl_1 = CnC_Isl.iloc[np.where((~(CnC_Isl.Loan_Acc_.isna()))&~(CnC_Isl.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA','Closing']].sum().reset_index()
        
        CnC_Isl_1['Loan_Acc_'] = CnC_Isl_1['Loan_Acc_'].astype(int)

        IA_Conv_1.rename(columns={'Closing_IA':'LAF_ECL_MYR',
                                'Closing':'LAF_ECL_FC'},inplace=True)

        IA_Isl_1.rename(columns={'Closing_IA':'LAF_ECL_MYR',
                                'Closing':'LAF_ECL_FC'},inplace=True)

        IA_IIS_1.rename(columns={'IIS_(RM)':'LAF_ECL_MYR',
                                'IIS_(FC)':'LAF_ECL_FC'},inplace=True)

        CnC_Conv_1.rename(columns={'Closing_IA':'CnC_ECL_MYR',
                                'Closing':'CnC_ECL_FC'},inplace=True)

        CnC_Isl_1.rename(columns={'Closing_IA':'CnC_ECL_MYR',
                                'Closing':'CnC_ECL_FC'},inplace=True)

        IA_Conv_1['Type_of_Financing'] = 'Conventional'
        IA_Isl_1['Type_of_Financing'] = 'Islamic'
        IA_IIS_1['Type_of_Financing'] = 'Conventional'
        CnC_Conv_1['Type_of_Financing'] = 'Conventional'
        CnC_Isl_1['Type_of_Financing'] = 'Islamic'

        IA_IIS_1.loc[IA_IIS_1.Borrower!="PT Mahakarya Inti Buana",'LAF_ECL_MYR']=0
        IA_IIS_1.loc[IA_IIS_1.Borrower!="PT Mahakarya Inti Buana",'LAF_ECL_FC']=0

        merge = pd.concat([IA_Conv_1,IA_Isl_1,IA_IIS_1,CnC_Conv_1,CnC_Isl_1])

        merge.fillna(0, inplace=True)

        merge['Loan_Acc_'] = merge['Loan_Acc_'].astype(str)
        #mergee['Ccy'] = merge['Ccy'].astype(float)
        #mergee['Borrower'] = merge['Borrower'].astype(float)
        #mergee['Type_of_Financing'] = merge['Type_of_Financing'].astype(float)
        merge['LAF_ECL_FC'] = merge['LAF_ECL_FC'].astype(float)
        merge['LAF_ECL_MYR'] = merge['LAF_ECL_MYR'].astype(float)
        merge['CnC_ECL_FC'] = merge['CnC_ECL_FC'].astype(float)
        merge['CnC_ECL_MYR'] = merge['CnC_ECL_MYR'].astype(float)

        appendfinal = merge.fillna(0).groupby(['Loan_Acc_'\
        ,'Borrower','Ccy','Type_of_Financing'])[['LAF_ECL_FC'\
        ,'LAF_ECL_MYR','CnC_ECL_FC','CnC_ECL_MYR']].sum().reset_index().drop_duplicates('Loan_Acc_', keep='first')

        appendfinal.rename(columns={'Loan_Acc_':'Account'},inplace=True)
        appendfinal['Account'] = appendfinal['Account'].astype(str)

        appendfinal['ECL_FC'] = appendfinal['LAF_ECL_FC'].fillna(0) + appendfinal['CnC_ECL_FC'].fillna(0)
        appendfinal['ECL_MYR'] = appendfinal['LAF_ECL_MYR'].fillna(0) + appendfinal['CnC_ECL_MYR'].fillna(0)

        convert_time = str(current_time).replace(":","-")
        appendfinal['position_as_at'] = reportingDate
        appendfinal.to_excel(config.FOLDER_CONFIG["FTP_directory"]+"Result_Allowance_"+str(convert_time)[:19]+".xlsx",index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

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
        cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel Allowance",uploadedByEmail))
        conn.commit()
        sql_error = """UPDATE [jobPython]
        SET [jobCompleted] = NULL, [jobErrDetail]= 'Process Excel Allowance'
        WHERE [jobName] = 'Allowance';
                    """
        cursor.execute(sql_error)
        conn.commit()
        print(f"Process Excel Allowance Error: {e}")
        sys.exit(f"Process Excel Allowance Error: {str(e)}")
        sys.exit(1) 

    #---------------------------------------------Download-------------------------------------------------------------

    try:
        # Assuming 'combine2' is a DataFrame
        column_types = []
        for col in appendfinal.columns:
            # You can choose to map column types based on data types in the DataFrame, for example:
            if appendfinal[col].dtype == 'object':  # String data type
                column_types.append(f"{col} VARCHAR(255)")
            elif appendfinal[col].dtype == 'int64':  # Integer data type
                column_types.append(f"{col} INT")
            elif appendfinal[col].dtype == 'float64':  # Float data type
                column_types.append(f"{col} FLOAT")
            else:
                column_types.append(f"{col} VARCHAR(255)")  # Default type for others


        # Generate the CREATE TABLE statement
        create_table_query = "CREATE TABLE A_ALLOWANCE (" + ', '.join(column_types) + ")"
        # Execute the query
        cursor.execute(create_table_query)

        for row in appendfinal.iterrows():
            sql = "INSERT INTO A_ALLOWANCE({}) VALUES ({})".format(','.join(appendfinal.columns), ','.join(['?']*len(appendfinal.columns)))
            cursor.execute(sql, tuple(row[1]))
        conn.commit()

        cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_ALLOWANCE AS source
        ON target.finance_sap_number = source.Account
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
        cursor.execute("drop table A_ALLOWANCE")
        conn.commit() 

        sql_query4 = """UPDATE [jobPython]
        SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
        WHERE [jobName] = 'Allowance';
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
        cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database Allowance",uploadedByEmail))
        conn.commit()
        sql_error = """UPDATE [jobPython]
        SET [jobCompleted] = NULL, [jobErrDetail]= 'Update Database Allowance'
        WHERE [jobName] = 'Allowance';
                    """
        cursor.execute(sql_error)
        conn.commit() 
        print(f"Update Database Allowance Error: {e}")
        sys.exit(f"Update Database Allowance Error: {str(e)}")
        sys.exit(1)
except Exception as e:
    print(f"Python Error: {e}")
    sys.exit(f"Python Error: {str(e)}")
    sys.exit(1)