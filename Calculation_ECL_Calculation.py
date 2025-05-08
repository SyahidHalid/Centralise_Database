# python Allowance.py 13 "Allowance_0225_Adjusted.xlsx" "Allowance" "Pending Processing" "0" "syahidhalid@exim.com.my" "2025-02-28"

#try:
import os
import sys
import pyodbc
import config

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
        #sys.exit(1)  # Exit the script with a failure code
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

    pd.set_option("display.max_columns", None) 
    pd.set_option("display.max_colwidth", 1000) #huruf dlm column
    pd.set_option("display.max_rows", 100)
    pd.set_option("display.precision", 2) #2 titik perpuluhan

except Exception as e:
    print(f"Library Error: {e}")
    sys.exit(f"Library Error: {str(e)}")
    #sys.exit(1)
        
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

    Active = pd.read_sql_query("SELECT * FROM table_ecl_report_data", conn)
    
    PD = pd.read_sql_query("SELECT * FROM ECL_PD_LIFETIME", conn)
    
    FL_PD = pd.read_sql_query("SELECT * FROM ECL_PD_FORWARD", conn)

    sql_query1 = """UPDATE [jobPython]
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Allowance.py',[jobCompleted] = NULL
    WHERE [jobName] = 'Allowance';
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
    #   Excel File Name
    Active.tail(3)
    #Active['position_as_at_date'].value_counts()
    #Active['account_status_id'].value_counts()
    #Active['lgd_rate'].value_counts()
    #Active['position_as_at_date'].dtypes

    Active['position_as_at_date'] = Active['position_as_at_date'].astype(str)

    Active1 = Active.iloc[np.where((Active['account_status_id'].isin([30947,
                                                                     30951,
                                                                     30949,
                                                                     30948,
                                                                     30950]))&
                                                                     (Active['position_as_at_date']=='2025-03-31'))]
    #Active1['account_status_id'].value_counts()
    #Active1 = Active.iloc[np.where((Active['position_as_at_date3']==reportingDate))]

    #Active1.to_excel(r"D:\\view_ecl_report_data_20250505_PROD_v1.xlsx",index=False)

    Active1 = Active1[["account_no",#"Finance (SAP) Number",
                     "borrower_name",#"Borrower name",
                     "first_released_date",#"First Released Date",
                     "maturity_date",#"Maturity date",
                     "availability_period_date",#"Availability period",
                     "revolving_type",#"Revolving/Non-revolving", cek code
                     "total_outstanding_base_currency",#"Total outstanding (base currency)",
                     "principal_payment_base_currency",#"Principal payment (base currency)",
                     "principal_payment_frequency",#"Principal payment frequency",
                     "interest_payment_base_currency",#"Interest payment (base currency)",
                     "interest_payment_frequency",#"Interest payment frequency",
                     "undrawn_amount_base_currency",#"Undrawn amount (base currency)",
                     "profit_rate_eir",#"Profit Rate/ EIR",
                     "pd_segment_value_final",#"PD segment",
                     #"",#"LGD Segment",
                     "lgd_rate",#"LGD rate",
                     "fl_segment_country_exposure_type_name",#"FL segment",
                     "facility_currency_code",#"Currency",
                     "dpd",#"DPD",
                     "watchlist",#"Watchlist (Yes/No)",
                     "corporate_sovereign_name",#"Corporate/Sovereign",
                     "fx_value"]]#"FX"]]

    #Active['Reporting date'] = reportingDate
    Active1['Reporting_date'] = '2025-03-31'
    
    # Date Format
    Active1["first_released_date"] = pd.to_datetime(Active1["first_released_date"], errors='coerce')
    Active1["maturity_date"] = pd.to_datetime(Active1["maturity_date"], errors='coerce')
    Active1["availability_period_date"] = pd.to_datetime(Active1["availability_period_date"], errors='coerce')
    Active1["Reporting_date"] = pd.to_datetime(Active1["Reporting_date"], errors='coerce')


    # YOB
    Active1["YOB"] = ((Active1["maturity_date"].dt.year - Active1["Reporting_date"].dt.year)*12+(Active1["maturity_date"].dt.month - Active1["Reporting_date"].dt.month))#+1
    


    
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
    #sys.exit(1)