# python CCRIS_Template.py 1, "a", "CCRIS Template", "Pending Processing", "0", "syahidhalid@exim.com.my","2025-07-31"

#   reportingDate = '2026-07-31' 
#   documentId = 1


#   Library
import os
import sys
import pyodbc
import config
import pandas as pd
import numpy as np
import datetime as dt
import xlsxwriter

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

    # BG_Hist.shape
    # BG_Hist.dtypes
    # BG_Hist['Guarantee No.'].value_counts()
    # BG_Hist.iloc[np.where(BG_Hist['Guarantee No.']=='EXIM/PFSB/BG-i/26/004')]

    BG_Hist = pd.read_sql_query(
        "SELECT * FROM bgHist WHERE positionAsAt = ?",
        conn,
        params=(reportingDate,)
    )

    sql_query1 = """UPDATE [jobPython]
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='CCRIS_Template_BG.py',[jobCompleted] = NULL
    WHERE [jobName] = 'CCRIS Template BG';
                """
    cursor.execute(sql_query1)
    conn.commit() 
except Exception as e:
    print(f"Connect to Database Error: {e}")
    sys.exit(f"Connect to Database Error: {str(e)}")
    #sys.exit(1)

#------------------------------------------------------------------------------------------------


#BG_combine['Borrower'].value_counts()
#BG_combine.iloc[np.where(BG_combine['Guarantee No.']=='EXIM/PFSB/BG-I/26/004' ) ].Borrower.value_counts()

#upload excel
try:
    # BG_Hist['Original Expiry Date'] = pd.to_datetime(BG_Hist['Original Expiry Date'],errors='coerce')
    # BG_Hist['Extended Expiry Date'] = pd.to_datetime(BG_Hist['Extended Expiry Date'],errors='coerce')

    # Borrower
    BG_Hist_group_name = BG_Hist[['Guarantee No.','Borrower']].drop_duplicates(subset=['Guarantee No.'], keep='last').reset_index(drop=True)

    # Amount Issued
    BG_hist_group = BG_Hist.groupby(['Guarantee No.'])[['Amount Issued']].sum().reset_index()

    BG_combine = BG_hist_group.merge(BG_Hist_group_name, how='left', on='Guarantee No.')

    BG_combine1 = BG_combine.iloc[np.where(BG_combine['Amount Issued']>0)]

    BG_combine1['Guarantee No.'] = BG_combine1['Guarantee No.'].str.upper()
    BG_combine1['Borrower'] = BG_combine1['Borrower'].str.upper()

    
    # BG_combine1.loc[(BG_combine1['Borrower'].str.contains('PRINSIPTEK'))&(BG_combine1['Original Expiry Date'].isnull()), 'Original Expiry Date'] = pd.to_datetime(reportingDate) + pd.DateOffset(years=1)
    # BG_combine1.loc[(BG_combine1['Extended Expiry Date']=="")|(BG_combine1['Extended Expiry Date'].isnull()), 'Extended Expiry Date'] = BG_combine1['Original Expiry Date']
    # BG_Hist1 = BG_combine.iloc[np.where((BG_combine['Extended Expiry Date']>=pd.to_datetime(reportingDate))&BG_combine['Amount Issued']>0)] #(BG_Hist['Extended Expiry Date']!="")&

    
    # BG_Hist.to_excel("a.xlsx", index=False)

    # Default
    BG_combine1["Instalment Amount (RM)"] = 0 
    BG_combine1["Number of instalment in arrears"] = 0 
    BG_combine1["Source of Repayment"] = ""
    BG_combine1["Type of Repayment"] = ""
    BG_combine1["Impaired Loan Recovered During the Month (RM)"] = 0
    BG_combine1["Impaired Loan Written-off During the Month (RM)"] = 0
    BG_combine1["Provision for Loan Sold to Danaharta (RM)"] = 0
    BG_combine1["Provision Transferred to Provision for Diminution in Value of Investment (RM)"] = 0
    BG_combine1["Number of instalment in arrears"] = 0
    BG_combine1["Loan Sold to Secondary Market under SBBA (RM)"] = 0
    BG_combine1["Date of Account Status"] = reportingDate


    BG_Hist2 = BG_combine1[['EXIM Account Number', # pickup from BG_Hist
                         'Borrower','Guarantee No.',
                         'positionAsAt', # pickup from BG_Hist
                         'Exposure (RM)', # pickup from BG_Hist
                         'Facility Limit Undrawn (MYR)', # pickup from BG_Hist
                         'Amount Issued',
                         'Instalment Amount (RM)',
                         'Source of Repayment',
                         'Type of Repayment',
                         'Impaired Loan Recovered During the Month (RM)',
                         'Impaired Loan Written-off During the Month (RM)',
                         'Provision for Loan Sold to Danaharta (RM)',
                         'Provision Transferred to Provision for Diminution in Value of Investment (RM)',
                         "Number of instalment in arrears","Date of Account Status",'Loan Sold to Secondary Market under SBBA (RM)']]

    
    BG_Hist2['EXIM Account Number'] = BG_Hist2['EXIM Account Number'].str.replace("-", "", regex=False)

    LDB_Hist = pd.read_sql_query("SELECT * FROM dbase_account_hist WHERE position_as_at = ?", conn, params=(reportingDate,))

    LDB_Hist1 = LDB_Hist[['facility_exim_account_num',
                          'cif_number',
                          'facility_application_sys_code_desc',
                          'facility_ccris_master_account_num',
                          'acc_accrued_interest_myr',
                          'acc_other_charges_myr',
                          'acc_contingent_liability_myr',
                          'int_month_in_arrears',
                          'acc_status_desc',
                          'acc_drawdown_myr',
                          'acc_repayment_myr',
                          'acc_interest_repayment_myr',
                          'penalty_repayment_myr',
                          'acc_margin',
                          'pd_percent',
                          'lgd_percent',
                          'acc_MFRS9_staging_desc',
                          'acc_credit_loss_cnc_ecl_myr',
                          'financing_type_desc']]


    combine = BG_Hist2.merge(LDB_Hist1, how='left', left_on='EXIM Account Number', right_on='facility_exim_account_num', indicator='_combine')
    #   combine._combine.value_counts()

    combine.sort_values('Borrower',ascending=True, inplace=True)

    combine['No'] = range(1, len(combine) + 1)

    # combine.financing_type_desc.value_counts()
    combine['penalty_repayment_myr_islamic'] = np.where(combine['financing_type_desc'] == 'Islamic', combine['penalty_repayment_myr'], 0)
    combine['penalty_repayment_myr_conventional'] = np.where(combine['financing_type_desc'] == 'Conventional', combine['penalty_repayment_myr'], 0)

    # combine.iloc[np.where(combine.acc_status_desc.isin(['Active','']) )]  
    combine1 = combine[['No',
                        'Borrower', # Customer Name
                        'cif_number', # Customer Number
                        'facility_application_sys_code_desc', # Application System Code
                        'facility_ccris_master_account_num', # Master Account Number
                        'Guarantee No.', # Sub Account Number
                        'positionAsAt', # Position Date
                        'Exposure (RM)', # Principal Outstanding (RM)
                        'acc_accrued_interest_myr', # Interest / Income Outstanding (RM) 
                        'acc_other_charges_myr', # Other Charges (RM)
                        'acc_contingent_liability_myr', # Total Outstanding (RM)
                        'int_month_in_arrears', # Months in arrears
                        'Number of instalment in arrears',
                        'acc_status_desc', # Account Status
                        'Loan Sold to Secondary Market under SBBA (RM)',
                        'Facility Limit Undrawn (MYR)', # Amount Undrawn (RM)
                        'acc_drawdown_myr', # Amount Disbursed During the Month (RM)
                        'acc_repayment_myr', # Amount Repaid During the Month (RM)
                        'Date of Account Status',
                        'Instalment Amount (RM)',
                        'penalty_repayment_myr_islamic', # Late Payment Charges for Ta'widh (Compensation) During the Month (RM)
                        'penalty_repayment_myr_conventional', # Late Payment Charges for Gharamah (Penalty) During the Month (RM)
                        'Source of Repayment',
                        'Type of Repayment',
                        'acc_margin', # Type of Estimates
                        'pd_percent', # Probability of Default (%)
                        'lgd_percent', # Loss Given Default (%)
                        'acc_MFRS9_staging_desc', # Classification of Exposures
                        'acc_credit_loss_cnc_ecl_myr', # Provision Amount (RM)
                        'Impaired Loan Recovered During the Month (RM)', 
                        'Impaired Loan Written-off During the Month (RM)',
                        'Provision for Loan Sold to Danaharta (RM)',
                        'Provision Transferred to Provision for Diminution in Value of Investment (RM)']]
    
    # combine1.to_excel("a.xlsx", index=False)

    #---------------------------------------------Details-------------------------------------------------------------
    
    # Extract
    # LDB4.head(1)
    # LDB4.shape
    convert_time = str(current_time).replace(":","-")
    #Loan Database
    writer2 = pd.ExcelWriter(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"CCRIS_Template_BG_"+str(convert_time)[:19]+".xlsx"),engine='xlsxwriter')

    combine1.to_excel(writer2, sheet_name='BG', index = False, startrow=7)

    writer2.close()

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'CCRIS Template BG';
                """
    cursor.execute(sql_query4)
    conn.commit() 

    #table    
    # documentId = 1    
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"CCRIS_Template_BG_"+str(convert_time)[:19]+".xlsx",'PY005','PY002')] #cari pakai code jgn pakai id ,36978,36960
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
    cursor.execute(sql_query3,(str(e)+" ["+str(documentName)+"]","Process Excel CCRIS Template BG",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobStatus]= 'PY004', [jobErrDetail]= 'Process Excel CCRIS Template BG'
    WHERE [jobName] = 'CCRIS Template BG';
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

    print(f"Process Excel CCRIS Template BGError: {e}")
    sys.exit(f"Process Excel CCRIS Template BG Error: {str(e)}")
