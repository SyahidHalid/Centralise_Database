# python Calculation_ECL_Computation.py 13 "ECL.xlsx" "ECL" "Pending Processing" "0" "syahidhalid@exim.com.my" "2025-01-31"

# 
#reportingDate = '2025-04-30'

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

    Active_before = pd.read_sql_query("SELECT * FROM table_ecl_report_data", conn)
    
    PD = pd.read_sql_query("SELECT * FROM ECL_PD_LIFETIME", conn)
    
    FL_PD = pd.read_sql_query("SELECT * FROM ECL_PD_FORWARD", conn)

    sql_query1 = """UPDATE [jobPython]
    SET [jobStartDate] = getdate(), [jobStatus]= 'PY001', [PythonFileName]='Calculation_ECL_Computation.py',[jobCompleted] = NULL
    WHERE [jobName] = 'Calculation ECL Computation';
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

    # Active.account_status_name.value_counts()
    
    # Active_before['lgd_rate'].value_counts()

    # Active_before['position_as_at_date'].value_counts()
    # Active_before['position_as_at_date'].dtypes

    Active_before['position_as_at_date'] = Active_before['position_as_at_date'].astype(str)

    Active_test = Active_before.iloc[np.where((Active_before['position_as_at_date']==reportingDate))]#['account_status_id'].value_counts()

    # Active_test["watchlist"].value_counts()
    # Active_test['account_status_id'].dtypes
    # Active_test['account_status_id'].value_counts()
    
    Active_test1 = Active_test.iloc[np.where((Active_test['account_status_id'].isin([30947,30952,30949,30948,30950])))]#['position_as_at_date3'].value_counts()

    #Active1.to_excel(r"D:\\view_ecl_report_data_20250505_PROD_v1.xlsx",index=False)


    Active = Active_test1[["account_no",#"Finance (SAP) Number",
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
                     "fx_value"]].rename(columns={'account_no':'Finance (SAP) Number',#"FX"]]
                                                  'borrower_name':'Borrower name',
                                                  'first_released_date':'First Released Date',
                                                  'maturity_date':'Maturity date',
                                                  'availability_period_date':'Availability period',
                                                  'revolving_type':'Revolving/Non-revolving',
                                                  'total_outstanding_base_currency':'Total outstanding (base currency)',
                                                  'principal_payment_base_currency':'Principal payment (base currency)',
                                                  'principal_payment_frequency':'Principal payment frequency',
                                                  'interest_payment_base_currency':'Interest payment (base currency)',
                                                  'interest_payment_frequency':'Interest payment frequency',
                                                  'undrawn_amount_base_currency':'Undrawn amount (base currency)',
                                                  'profit_rate_eir':'Profit Rate/ EIR',
                                                  'pd_segment_value_final':'PD segment',
                                                  'lgd_rate':'LGD rate',
                                                  'fl_segment_country_exposure_type_name':'FL segment',
                                                  'facility_currency_code':'Currency',
                                                  'dpd':'DPD',
                                                  'watchlist':'Watchlist (Yes/No)',
                                                  'corporate_sovereign_name':'Corporate/Sovereign',
                                                  'fx_value':'FX'})

    Active['Reporting date'] = reportingDate


    # Date Format
    Active["First Released Date"] = pd.to_datetime(Active["First Released Date"], errors='coerce')
    Active["Maturity date"] = pd.to_datetime(Active["Maturity date"], errors='coerce')
    Active["Availability period"] = pd.to_datetime(Active["Availability period"], errors='coerce')
    Active["Reporting date"] = pd.to_datetime(Active["Reporting date"], errors='coerce')

    
    # YOB
    Active["YOB"] = ((Active["Maturity date"].dt.year - Active["Reporting date"].dt.year)*12+(Active["Maturity date"].dt.month - Active["Reporting date"].dt.month))#+1
    
    #Active["YOB"].fillna(0,inplace=True)
    #Active["YOB"] = Active["YOB"].astype(int)

    def extend_row(row):
        # Create a new DataFrame for the row repeated `Value + 1` times
        repeated_rows = pd.DataFrame([row] * (row['YOB'] + 1))
        # Add a new column for the sequence
        repeated_rows['Sequence'] = range(row['YOB'] + 1)
        return repeated_rows
    # Apply the extend_row function for each row and concatenate the results
    extended_Active = pd.concat([extend_row(row) for index, row in Active.iterrows()], ignore_index=True)

    #Principal
        #=IF(D30="",0,
    #   (IF(AND($D$14="Bullet",MOD(D30,(((YEAR($D$7)-YEAR($D$6))*12)+(MONTH($D$7)-MONTH($D$6))))>0),0,
    #     IF(AND($D$14="Quarterly",MOD(D30,3)>0),0,
    #       IF(AND($D$14="Semi Annually",MOD(D30,6)>0),0,
    #         IF(AND($D$14="Annually",MOD(D30,12)>0),0,$D$13))))))
    extended_Active.loc[extended_Active["Principal payment (base currency)"]=="-","Principal payment (base currency)"] = 0
    extended_Active["Principal payment (base currency)"] = extended_Active["Principal payment (base currency)"].astype(float)
    extended_Active.loc[extended_Active["Sequence"]==0,"Cal_Principal_payment"] = 0

    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Annually"]))&((extended_Active["Sequence"]%12)>0),"Cal_Principal_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Annually"]))&((extended_Active["Sequence"]%12)==0),"Cal_Principal_payment"] = extended_Active["Principal payment (base currency)"]

    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Semi Annually"]))&((extended_Active["Sequence"]%6)>0),"Cal_Principal_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Semi Annually"]))&((extended_Active["Sequence"]%6)==0),"Cal_Principal_payment"] = extended_Active["Principal payment (base currency)"]
    
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Quarterly"]))&((extended_Active["Sequence"]%3)>0),"Cal_Principal_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Quarterly"]))&((extended_Active["Sequence"]%3)==0),"Cal_Principal_payment"] = extended_Active["Principal payment (base currency)"]
    
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Bullet"]))&(extended_Active["Sequence"]!=extended_Active["YOB"]),"Cal_Principal_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Principal payment frequency"].isin(["Bullet"]))&(extended_Active["Sequence"]==extended_Active["YOB"]),"Cal_Principal_payment"] = extended_Active["Principal payment (base currency)"]
    
    extended_Active.loc[(extended_Active["Sequence"]!=0)&~(extended_Active["Principal payment frequency"].isin(["Bullet","Quarterly","Semi Annually","Annually"])),"Cal_Principal_payment"] = extended_Active["Principal payment (base currency)"]

    extended_Active['Cummulative_Cal_Principal_payment'] = extended_Active.groupby('Finance (SAP) Number')['Cal_Principal_payment'].cumsum()


    #Interest
    #=IFERROR(IF(D30>=$D$9,I29,
    #           IF(D30="","",
    #             IF(SUM(E30+F30)>I29,I29,
    #               SUM(E30+F30)))),"")
    extended_Active.loc[extended_Active["Interest payment (base currency)"]=="-","Interest payment (base currency)"] = 0
    extended_Active["Interest payment (base currency)"] = extended_Active["Interest payment (base currency)"].astype(float)
    extended_Active.loc[extended_Active["Sequence"]==0,"Cal_Interest_payment"] = 0

    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Annually"]))&((extended_Active["Sequence"]%12)>0),"Cal_Interest_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Annually"]))&((extended_Active["Sequence"]%12)==0),"Cal_Interest_payment"] = extended_Active["Interest payment (base currency)"]

    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Semi Annually"]))&((extended_Active["Sequence"]%6)>0),"Cal_Interest_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Semi Annually"]))&((extended_Active["Sequence"]%6)==0),"Cal_Interest_payment"] = extended_Active["Interest payment (base currency)"]
    
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Quarterly"]))&((extended_Active["Sequence"]%3)>0),"Cal_Interest_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Quarterly"]))&((extended_Active["Sequence"]%3)==0),"Cal_Interest_payment"] = extended_Active["Interest payment (base currency)"]
    
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Bullet"]))&(extended_Active["Sequence"]!=extended_Active["YOB"]),"Cal_Interest_payment"] = 0
    extended_Active.loc[(extended_Active["Sequence"]!=0)&(extended_Active["Interest payment frequency"].isin(["Bullet"]))&(extended_Active["Sequence"]==extended_Active["YOB"]),"Cal_Interest_payment"] = extended_Active["Interest payment (base currency)"]
    
    extended_Active.loc[(extended_Active["Sequence"]!=0)&~(extended_Active["Interest payment frequency"].isin(["Bullet","Quarterly","Semi Annually","Annually"])),"Cal_Interest_payment"] = extended_Active["Interest payment (base currency)"]

    extended_Active['Cummulative_Cal_Interest_payment'] = extended_Active.groupby('Finance (SAP) Number')['Cal_Interest_payment'].cumsum()


    #Undrawn
    #=IF(D30=0,0,
    #   IF($D$12=0,0,
    #     IF(AND($D$10="Non-revolving",D30<=((YEAR($D$8)-YEAR($D$6))*12)+(MONTH($D$8)-MONTH($D$6))),($D$12)/(((YEAR($D$8)-YEAR($D$6))*12)+(MONTH($D$8)-MONTH($D$6))),
    #       IF(AND($D$10="Revolving",D30<=12,$D$9>12),$D$12/12,IF(AND($D$10="Revolving",D30<=($D$9-1),$D$9<=12),
    #         IFERROR(($D$12/($D$9-1)),$D$12/$D$9),0)))))
    extended_Active.loc[extended_Active["Undrawn amount (base currency)"]==0,"Undrawn_balance"] = 0

    extended_Active.loc[(extended_Active["Revolving/Non-revolving"]=="Non-revolving")&(extended_Active["Sequence"]<=extended_Active["YOB"]),"Undrawn_balance"] = extended_Active["Undrawn amount (base currency)"]/extended_Active["YOB"]
    extended_Active.loc[(extended_Active["Revolving/Non-revolving"]=="Revolving")&(extended_Active["Sequence"]<=12)&(extended_Active["YOB"]>12),"Undrawn_balance"] = extended_Active["Undrawn amount (base currency)"]/(extended_Active["YOB"])
    extended_Active.loc[(extended_Active["Revolving/Non-revolving"]=="Revolving")&(extended_Active["Sequence"]<=extended_Active["YOB"]-1)&(extended_Active["YOB"]<=12),"Undrawn_balance"] = extended_Active["Undrawn amount (base currency)"]/(extended_Active["YOB"]-1)
    extended_Active["Undrawn_balance"].fillna(0, inplace=True)

    extended_Active.loc[extended_Active["Sequence"]==0,"Undrawn_balance"] = 0
    extended_Active['Cummulative_Undrawn_balance'] = extended_Active.groupby('Finance (SAP) Number')['Undrawn_balance'].cumsum()
    

    #Installment
    #=IFERROR(IF(D30>=$D$9,I29,
    #           IF(D30="","",
    #             IF(SUM(E30+F30)>I29,I29,
    #                 SUM(E30+F30)))),"")
    extended_Active["Instalment Amount"] = extended_Active["Cal_Principal_payment"]+extended_Active["Cal_Interest_payment"]
    extended_Active["Instalment Amount (C&C)"] = extended_Active["Cal_Principal_payment"]+extended_Active["Cal_Interest_payment"]
    #extended_Active.loc[extended_Active["Instalment Amount (C&C)"]>extended_Active["Instalment Amount (C&C)"].shift(1),"Instalment Amount (C&C)"] = extended_Active["Instalment Amount (C&C)"].shift(1)
    
    extended_Active['Cummulative_Instalment_Amount'] = extended_Active.groupby('Finance (SAP) Number')['Instalment Amount'].cumsum()
    extended_Active['Cummulative_Instalment_Amount_C&C'] = extended_Active.groupby('Finance (SAP) Number')['Instalment Amount (C&C)'].cumsum()


    #Outstanding balance and Undisbursed @ EAD
    #=IF(D32="","",
    #   I31-G32+H32)
    #     EAD = "OS + (Undisbursed * CCF)
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Monthly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Monthly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"]) #

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"])#

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Semi Annually"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Semi Annually"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Bullet"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Bullet"),"EAD (C&C)"] = 0+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = 0+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"])#/(extended_Active["Sequence"])
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["EAD (C&C)"]/(extended_Active["Sequence"])
    
    extended_Active.loc[(extended_Active["Principal payment frequency"]==0)&(extended_Active["Interest payment frequency"]==0),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]==0)&(extended_Active["Interest payment frequency"]==0),"EAD (C&C)"] = 0+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"])#/(extended_Active["Sequence"])

    extended_Active.loc[extended_Active["EAD"]<0,"EAD"] = 0
    extended_Active.loc[extended_Active["EAD (C&C)"]<0,"EAD (C&C)"] = 0


    #sambungan installment
    extended_Active.loc[extended_Active["Sequence"]>=extended_Active["YOB"],"Instalment Amount"] = extended_Active["EAD"].shift(1)
    extended_Active.loc[extended_Active["Sequence"]>=extended_Active["YOB"],"Instalment Amount (C&C)"] = extended_Active["EAD (C&C)"].shift(1)

    extended_Active.loc[extended_Active["Instalment Amount"]>extended_Active["EAD"].shift(1),"Instalment Amount"] = extended_Active["EAD"].shift(1)
    extended_Active.loc[extended_Active["Instalment Amount (C&C)"]>extended_Active["EAD (C&C)"].shift(1),"Instalment Amount (C&C)"] = extended_Active["EAD (C&C)"].shift(1)

    extended_Active['Cummulative_Instalment_Amount'] = extended_Active.groupby('Finance (SAP) Number')['Instalment Amount'].cumsum()
    extended_Active['Cummulative_Instalment_Amount_C&C'] = extended_Active.groupby('Finance (SAP) Number')['Instalment Amount (C&C)'].cumsum()


    #Outstanding balance and Undisbursed @ EAD (2)
    #=IF(D32="","",
    #   I31-G32+H32)
    #     EAD = "OS + (Undisbursed * CCF)
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Monthly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Monthly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"]) #

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Quarterly")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"])#

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Semi Annually"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Semi Annually"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")

    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD"] = extended_Active["Total outstanding (base currency)"]-(extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Semi Annually")&(extended_Active["Interest payment frequency"]=="Quarterly"),"EAD (C&C)"] = extended_Active["Instalment Amount (C&C)"]-(extended_Active["Cummulative_Instalment_Amount_C&C"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Bullet"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Bullet"),"EAD (C&C)"] = 0+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = 0+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"])#/(extended_Active["Sequence"])
    extended_Active.loc[(extended_Active["Principal payment frequency"]=="Bullet")&(extended_Active["Interest payment frequency"]=="Monthly"),"EAD (C&C)"] = extended_Active["EAD (C&C)"]/(extended_Active["Sequence"])
    
    extended_Active.loc[(extended_Active["Principal payment frequency"]==0)&(extended_Active["Interest payment frequency"]==0),"EAD"] = extended_Active["Total outstanding (base currency)"]+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount"]) #&(extended_Active["Interest payment frequency"]=="Quarterly")
    extended_Active.loc[(extended_Active["Principal payment frequency"]==0)&(extended_Active["Interest payment frequency"]==0),"EAD (C&C)"] = 0+(extended_Active["Cummulative_Undrawn_balance"]-extended_Active["Cummulative_Instalment_Amount_C&C"])#/(extended_Active["Sequence"])
    
    extended_Active.loc[extended_Active["EAD"]<0,"EAD"] = 0
    extended_Active.loc[extended_Active["EAD (C&C)"]<0,"EAD (C&C)"] = 0


    PD.PD = PD.PD.str.upper()
    Pivoted_PD = PD

    FL_PD.PD = FL_PD.PD.str.upper()
    Pivoted_FL_PD = FL_PD

    #Pivoted_PD = PD.melt(id_vars="PD",value_vars=[1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13	,14	,15	,16	,17	,18	,19	,20	,21	,22	,23	,24	,25	,26	,27	,28	,29	,30	,31	,32	,33	,34	,35	,36	,37	,38	,39	,40	,41	,42	,43	,44	,45	,46	,47	,48	,49	,50	,51	,52	,53	,54	,55	,56	,57	,58	,59	,60	,61	,62	,63	,64	,65	,66	,67	,68	,69	,70	,71	,72	,73	,74	,75	,76	,77	,78	,79	,80	,81	,82	,83	,84	,85	,86	,87	,88	,89	,90	,91	,92	,93	,94	,95	,96	,97	,98	,99	,100	,101	,102	,103	,104	,105	,106	,107	,108	,109	,110 ,111	,112	,113	,114	,115	,116	,117	,118	,119	,120	,121	,122	,123	,124	,125	,126	,127	,128	,129	,130	,131	,132	,133	,134	,135	,136	,137	,138	,139	,140	,141	,142	,143	,144	,145	,146	,147	,148	,149	,150	,151	,152	,153	,154	,155	,156	,157	,158	,159	,160	,161	,162	,163	,164	,165	,166	,167	,168	,169	,170	,171	,172	,173	,174	,175	,176	,177	,178	,179,180],var_name="Year",value_name="PD%")
    #Pivoted_FL_PD = FL_PD.melt(id_vars="PD",value_vars=[1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13	,14	,15	,16	,17	,18	,19	,20	,21	,22	,23	,24	,25	,26	,27	,28	,29	,30	,31	,32	,33	,34	,35	,36	,37	,38	,39	,40	,41	,42	,43	,44	,45	,46	,47	,48	,49	,50	,51	,52	,53	,54	,55	,56	,57	,58	,59	,60	,61	,62	,63	,64	,65	,66	,67	,68	,69	,70	,71	,72	,73	,74	,75	,76	,77	,78	,79	,80	,81	,82	,83	,84	,85	,86	,87	,88	,89	,90	,91	,92	,93	,94	,95	,96	,97	,98	,99	,100	,101	,102	,103	,104	,105	,106	,107	,108	,109	,110 ,111	,112	,113	,114	,115	,116	,117	,118	,119	,120	,121	,122	,123	,124	,125	,126	,127	,128	,129	,130	,131	,132	,133	,134	,135	,136	,137	,138	,139	,140	,141	,142	,143	,144	,145	,146	,147	,148	,149	,150	,151	,152	,153	,154	,155	,156	,157	,158	,159	,160	,161	,162	,163	,164	,165	,166	,167	,168	,169	,170	,171	,172	,173	,174	,175	,176	,177	,178	,179,180],var_name="Year",value_name="FL PD%")
    #st.write(Pivoted_PD)

    extended_Active["PD segment"] = extended_Active["PD segment"].astype(str)
    extended_Active["Sequence"] = extended_Active["Sequence"].astype(str)

    Pivoted_PD["PD"] = Pivoted_PD["PD"].astype(str)
    Pivoted_PD["Year"] = Pivoted_PD["Year"].astype(str)

    extended_Active_PD = extended_Active.merge(Pivoted_PD.rename(columns={"PD":"PD segment","Year":"Sequence"}),on=["PD segment","Sequence"],how="left") #,indicator=True

    import string
    extended_Active_PD['Number'] = range(1, len(extended_Active_PD) + 1)
    extended_Active_PD['Key'] = [string.ascii_uppercase[i % len(string.ascii_uppercase)] for i in range(len(extended_Active_PD))]

    # st.write(extended_Active_PD)
    # st.write(extended_Active_PD.shape)
    # st.write("")
    # st.download_button("Download CSV",
    #                  extended_Active_PD.to_csv(index=False),
    #                  file_name='extended_Active_PD.csv',
    #                  mime='text/csv')
    

    # EIR
    Active_EIR = Active

    # Only apply date_range where both dates are present
    Active_EIR['month_ends'] = Active_EIR.apply(
        lambda row: pd.date_range(start=row['Reporting date'], end=row['Maturity date'], freq='M')
        if pd.notna(row['Reporting date']) and pd.notna(row['Maturity date']) else pd.NaT,
        axis=1
    )


  
    # Function to adjust month_ends and include the actual end date if it's not a month-end
    def adjust_month_ends(row):
        month_ends = row['month_ends']

        end_date = pd.to_datetime(row['Maturity date'])
        
        # Check if the end date is a month-end, if not, append it
        if end_date.is_month_end:
            return list(month_ends)
        else:
            return list(month_ends.union([end_date]))

    # Apply the adjustment to each row and convert to a list of Timestamps
    Active_EIR['adjusted_month_ends'] = Active_EIR.apply(adjust_month_ends, axis=1)

    # Convert month_ends column to lists of Timestamps to avoid Arrow errors
    Active_EIR['month_ends'] = Active_EIR['month_ends'].apply(list)

    Active_EIR = Active_EIR.explode('adjusted_month_ends')
    Active_EIR = Active_EIR.reset_index(drop=True)
    Active_EIR['month_ends_shift'] =  Active_EIR['adjusted_month_ends'].shift(1)
    Active_EIR["Sequence 1"] = (Active_EIR['adjusted_month_ends'] - Active_EIR['month_ends_shift']).dt.days
    Active_EIR.loc[Active_EIR['month_ends_shift'].isna(),"Sequence 1"] = 0
    Active_EIR.loc[Active_EIR['Sequence 1']<0,"Sequence 1"] = 0

    Active_EIR.loc[Active_EIR['Sequence 1']==0,"month_ends_shift"] = Active_EIR['Reporting date']
    Active_EIR["Sequence 2"] = (Active_EIR['adjusted_month_ends'] - Active_EIR['month_ends_shift']).dt.days
    
    Active_EIR = Active_EIR[["Finance (SAP) Number","YOB","adjusted_month_ends","month_ends_shift","Sequence 2"]]

    #st.write(Active_EIR)

    import string
    Active_EIR['Number'] = range(1, len(Active_EIR) + 1)
    Active_EIR['Key'] = [string.ascii_uppercase[i % len(string.ascii_uppercase)] for i in range(len(Active_EIR))]

    extended_Active_PD_1 = extended_Active_PD.merge(Active_EIR,on=['Finance (SAP) Number','YOB','Number','Key'],how="left")

    extended_Active_PD_1.rename(columns={"Sequence 2":"NOD"},inplace=True)

    extended_Active_PD_1['Prev_Cumulative'] = extended_Active_PD_1.groupby('Finance (SAP) Number')['NOD'].cumsum()
    
    extended_Active_PD_1['Prev_Cumulative'].fillna(0, inplace=True)

    extended_Active_PD_1["Profit Rate/ EIR"] = extended_Active_PD_1["Profit Rate/ EIR"].astype(float)
    extended_Active_PD_1["Prev_Cumulative"] = extended_Active_PD_1["Prev_Cumulative"].astype(float)

    extended_Active_PD_1["EIR adj"] =1/((1+extended_Active_PD_1["Profit Rate/ EIR"])**((extended_Active_PD_1["Prev_Cumulative"])/365)) #30.5 number of day in a month
    

    #ECL
    extended_Active_PD_1["EIR adj"]=extended_Active_PD_1["EIR adj"].astype(float)
    extended_Active_PD_1["LGD rate"]=extended_Active_PD_1["LGD rate"].astype(float)
    extended_Active_PD_1["PD_PERCENTAGE"]=extended_Active_PD_1["PD_PERCENTAGE"].astype(float)
    extended_Active_PD_1["EAD"]=extended_Active_PD_1["EAD"].astype(float)
    extended_Active_PD_1["FX"]=extended_Active_PD_1["FX"].astype(float)

    extended_Active_PD_1["S1 ECL (Overall) FC"] = extended_Active_PD_1["EAD"]*extended_Active_PD_1["PD_PERCENTAGE"]*extended_Active_PD_1["LGD rate"]*extended_Active_PD_1["EIR adj"]
    extended_Active_PD_1["S1 ECL (Overall) MYR"] = extended_Active_PD_1["S1 ECL (Overall) FC"]*extended_Active_PD_1["FX"]

    extended_Active_PD_1["S1 ECL (C&C) FC"] = extended_Active_PD_1["EAD (C&C)"]*extended_Active_PD_1["PD_PERCENTAGE"]*extended_Active_PD_1["LGD rate"]*extended_Active_PD_1["EIR adj"]  
    extended_Active_PD_1["S1 ECL (C&C) MYR"] = extended_Active_PD_1["S1 ECL (C&C) FC"]*extended_Active_PD_1["FX"]

    extended_Active_PD_1.loc[(extended_Active_PD_1["Finance (SAP) Number"].isin([500724,500640,500642])),"S1 ECL (C&C) FC"] = 0
    
    extended_Active_PD_1["S1 ECL (LAF) FC"] = extended_Active_PD_1["S1 ECL (Overall) FC"] - extended_Active_PD_1["S1 ECL (C&C) FC"]
    extended_Active_PD_1["S1 ECL (LAF) MYR"] = extended_Active_PD_1["S1 ECL (LAF) FC"]*extended_Active_PD_1["FX"]


    #FL
    extended_Active_FL_PD = extended_Active_PD_1.merge(Pivoted_FL_PD.rename(columns={"PD":"PD segment","Year":"Sequence"}),on=["PD segment","Sequence"],how="left") #,indicator=True

    extended_Active_FL_PD["S2 ECL (Overall) FC"] = extended_Active_FL_PD["EAD"]*extended_Active_FL_PD["FL_PD_PERCENTAGE"]*extended_Active_FL_PD["LGD rate"]*extended_Active_FL_PD["EIR adj"]
    extended_Active_FL_PD["S2 ECL (Overall) MYR"] = extended_Active_FL_PD["S2 ECL (Overall) FC"]*extended_Active_FL_PD["FX"]

    extended_Active_FL_PD["S2 ECL (C&C) FC"] = extended_Active_FL_PD["EAD (C&C)"]*extended_Active_FL_PD["FL_PD_PERCENTAGE"]*extended_Active_FL_PD["LGD rate"]*extended_Active_FL_PD["EIR adj"]
    extended_Active_FL_PD["S2 ECL (C&C) MYR"] = extended_Active_FL_PD["S2 ECL (C&C) FC"]*extended_Active_FL_PD["FX"]

    extended_Active_FL_PD.loc[(extended_Active_FL_PD["Finance (SAP) Number"].isin(["500724","500640","500642"])),"S2 ECL (C&C) FC"] = 0

    extended_Active_FL_PD["S2 ECL (LAF) FC"] = extended_Active_FL_PD["S2 ECL (Overall) FC"] - extended_Active_FL_PD["S2 ECL (C&C) FC"]
    extended_Active_FL_PD["S2 ECL (LAF) MYR"] = extended_Active_FL_PD["S2 ECL (LAF) FC"]*extended_Active_FL_PD["FX"]


    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="No","Total ECL FC (LAF)"] = extended_Active_FL_PD["S1 ECL (LAF) FC"] 
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="No","Total ECL FC (C&C)"] = extended_Active_FL_PD["S1 ECL (C&C) FC"]
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="No","Total ECL FC (Overall)"] = extended_Active_FL_PD["S1 ECL (Overall) FC"]
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="Yes","Total ECL FC (LAF)"] = extended_Active_FL_PD["S2 ECL (LAF) FC"] 
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="Yes","Total ECL FC (C&C)"] = extended_Active_FL_PD["S2 ECL (C&C) FC"]
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="Yes","Total ECL FC (Overall)"] = extended_Active_FL_PD["S2 ECL (Overall) FC"]


    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="No","Total ECL MYR (LAF)"] = extended_Active_FL_PD["S1 ECL (LAF) MYR"] 
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="No","Total ECL MYR (C&C)"] = extended_Active_FL_PD["S1 ECL (C&C) MYR"]
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="No","Total ECL MYR (Overall)"] = extended_Active_FL_PD["S1 ECL (Overall) MYR"]
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="Yes","Total ECL MYR (LAF)"] = extended_Active_FL_PD["S2 ECL (LAF) MYR"] 
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="Yes","Total ECL MYR (C&C)"] = extended_Active_FL_PD["S2 ECL (C&C) MYR"]
    extended_Active_FL_PD.loc[extended_Active_FL_PD["Watchlist (Yes/No)"]=="Yes","Total ECL MYR (Overall)"] = extended_Active_FL_PD["S2 ECL (Overall) MYR"]


    # rule for active & watchlist
    extended_Active_FL_PD["Sequence"] = extended_Active_FL_PD["Sequence"].astype(int)

    ECL_Filter = extended_Active_FL_PD.iloc[np.where((extended_Active_FL_PD["Watchlist (Yes/No)"]=="No")&((extended_Active_FL_PD["Sequence"]<=12))|(extended_Active_FL_PD["Watchlist (Yes/No)"]=="Yes")|(extended_Active_FL_PD["Watchlist (Yes/No)"]=="Imp"))]

    
    ECL_Filter.loc[((ECL_Filter['Watchlist (Yes/No)'].isin(["Imp"]))),"Total ECL FC (LAF)"]=0
    ECL_Filter.loc[((ECL_Filter['Watchlist (Yes/No)'].isin(["Imp"]))),"Total ECL MYR (LAF)"]=0
    ECL_Filter.loc[((ECL_Filter['Watchlist (Yes/No)'].isin(["Imp"]))),"Total ECL FC (C&C)"]=0
    ECL_Filter.loc[((ECL_Filter['Watchlist (Yes/No)'].isin(["Imp"]))),"Total ECL MYR (C&C)"]=0
    ECL_Filter.loc[((ECL_Filter['Watchlist (Yes/No)'].isin(["Imp"]))),"Total ECL FC (Overall)"]=0
    ECL_Filter.loc[((ECL_Filter['Watchlist (Yes/No)'].isin(["Imp"]))),"Total ECL MYR (Overall)"]=0

    ECL_Group = ECL_Filter.groupby(["Finance (SAP) Number","Currency","Borrower name","Watchlist (Yes/No)"])[["Total ECL FC (LAF)","Total ECL MYR (LAF)",
                                                                                "Total ECL FC (C&C)","Total ECL MYR (C&C)",
                                                                                "Total ECL FC (Overall)","Total ECL MYR (Overall)"]].sum().reset_index()

    ECL_Group = ECL_Group.rename(columns={'Finance (SAP) Number':'Account_No',
                                          'Borrower name':'Borrower',
                                          'Total ECL FC (LAF)':'LAF_ECL_FC',
                                          'Total ECL MYR (LAF)':'LAF_ECL_MYR',
                                          'Total ECL FC (C&C)':'CnC_ECL_FC',
                                          'Total ECL MYR (C&C)':'CnC_ECL_MYR',
                                          'Total ECL FC (Overall)':'ECL_FC',
                                          'Total ECL MYR (Overall)':'ECL_MYR',
                                          'Reporting date':'position_as_at',})


    # Extract
    writer2 = pd.ExcelWriter(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_Calculation_ECL_Computation_"+str(reportingDate)[:19]+".xlsx"),engine='xlsxwriter')

    ECL_Filter.to_excel(writer2, sheet_name='ECL_CALCULATOR', index = False)

    ECL_Group.to_excel(writer2, sheet_name='ECL_SUMMARY', index = False)

    writer2.close()

    #table        
    columns = ['aftd_id','result_file_name','processed_status_id','status_id']
    data = [(documentId,"Result_Calculation_ECL_Computation_"+str(reportingDate)[:19]+".xlsx",'PY005','PY002')]
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

    sql_query4 = """UPDATE [jobPython]
    SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
    WHERE [jobName] = 'Calculation ECL Computation';
                """
    cursor.execute(sql_query4)
    conn.commit() 

    print("Data updated successfully at "+str(reportingDate))
    conn.close()
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
    cursor.execute(sql_query2,(str(e)+" ["+str(documentName)+"]","Upload Excel ECL",uploadedByEmail))
    conn.commit()
    sql_error = """UPDATE [jobPython]
    SET [jobCompleted] = NULL, [jobErrDetail]= 'Process ECL'
    WHERE [jobName] = 'ECL';
                """
    cursor.execute(sql_error)
    conn.commit()
    print(f"Process ECL Error: {e}")
    sys.exit(f"Process ECL Error: {str(e)}")
    #sys.exit(1)



#--------------------------------------------------------connect ngan database-----------------------------------------------------------------------------------------------------------------------------------------------------

# cntrl + K + C untuk comment kn sume 
# cntrl + K + U untuk comment kn sume 

# try:
#     #table        
#     columns = ['aftd_id','result_file_name','processed_status_id','status_id']
#     data = [(documentId,"Result_ECL_to_MIS_"+str(reportingDate)[:19]+".xlsx",'PY005','PY002')]
#     download_result = pd.DataFrame(data,columns=columns)
    
#     # Assuming 'combine2' is a DataFrame
#     column_types1 = []
#     for col in download_result.columns:
#         # You can choose to map column types based on data types in the DataFrame, for example:
#         if download_result[col].dtype == 'object':  # String data type
#             column_types1.append(f"{col} VARCHAR(255)")
#         elif download_result[col].dtype == 'int64':  # Integer data type
#             column_types1.append(f"{col} INT")
#         elif download_result[col].dtype == 'float64':  # Float data type
#             column_types1.append(f"{col} FLOAT")
#         else:
#             column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

#     create_table_query_result = "CREATE TABLE A_download_result_D (" + ', '.join(column_types1) + ")"
#     cursor.execute(create_table_query_result)

#     for row in download_result.iterrows():
#         sql_result = "INSERT INTO A_download_result_D({}) VALUES ({})".format(','.join(download_result.columns), ','.join(['?']*len(download_result.columns)))
#         cursor.execute(sql_result, tuple(row[1]))
#     conn.commit()


#     cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
#                     USING A_download_result_D AS source
#                     ON target.aftd_id = source.aftd_id
#                     WHEN MATCHED THEN 
#                         UPDATE SET target.result_file_name = source.result_file_name,
#                         target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
#                         target.status_id = (select param_id from param_system_param where param_code=source.status_id);   
#     """)
#     conn.commit() 
#     cursor.execute("drop table A_download_result_D")
#     conn.commit() 

#     #status id PY002
#     #processed_status_id PY005

#     #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#     # Assuming 'combine2' is a DataFrame
#     column_types = []
#     for col in ECL_Group.columns:
#         # You can choose to map column types based on data types in the DataFrame, for example:
#         if ECL_Group[col].dtype == 'object':  # String data type
#             column_types.append(f"{col} VARCHAR(255)")
#         elif ECL_Group[col].dtype == 'int64':  # Integer data type
#             column_types.append(f"{col} INT")
#         elif ECL_Group[col].dtype == 'float64':  # Float data type
#             column_types.append(f"{col} FLOAT")
#         else:
#             column_types.append(f"{col} VARCHAR(255)")  # Default type for others


#     # Generate the CREATE TABLE statement
#     create_table_query = "CREATE TABLE A_ECL_TO_MIS (" + ', '.join(column_types) + ")"
#     # Execute the query
#     cursor.execute(create_table_query)

#     for row in ECL_Group.iterrows():
#         sql = "INSERT INTO A_ECL_TO_MIS({}) VALUES ({})".format(','.join(ECL_Group.columns), ','.join(['?']*len(ECL_Group.columns)))
#         cursor.execute(sql, tuple(row[1]))
#     conn.commit()

#     cursor.execute("""MERGE INTO col_facilities_application_master AS target USING A_ECL_TO_MIS AS source
#     ON target.finance_sap_number = source.Account_No
#     WHEN MATCHED THEN
#         UPDATE SET target.acc_credit_loss_laf_ecl = source.LAF_ECL_FC,
#                 target.acc_credit_loss_laf_ecl_myr = source.LAF_ECL_MYR,
#                 target.acc_credit_loss_cnc_ecl = source.CnC_ECL_FC,
#                 target.acc_credit_loss_cnc_ecl_myr = source.CnC_ECL_MYR,
#                 target.acc_credit_loss_lafcnc_ecl = source.ECL_FC,
#                 target.acc_credit_loss_lafcnc_ecl_myr = source.ECL_MYR,
#                 target.position_as_at = source.position_as_at;
#     """)
#     conn.commit() 
#     cursor.execute("drop table A_ECL_TO_MIS")
#     conn.commit() 

#     sql_query4 = """UPDATE [jobPython]
#     SET [jobCompleted] = getdate(), [jobStatus]= 'PY002', [jobErrDetail]=NULL
#     WHERE [jobName] = 'ECL';
#                 """
#     cursor.execute(sql_query4)
#     conn.commit() 

#     print("Data updated successfully at "+str(reportingDate))
#     conn.close()
# except Exception as e:
#     print(f"Update Database Error: {e}")
#     sql_query5 = """INSERT INTO [log_apps_error] (
#                     [logerror_desc],
#                     [iduser],
#                     [dateerror],
#                     [page],
#                     [user_name]
#                 )
#                 VALUES
#                     (?,  
#                     0,  
#                     getdate(),  
#                     ?,  
#                     ?
#                     )
#                 """
#     cursor.execute(sql_query5,(str(e)+" ["+str(documentName)+"]","Update Database ECL ",uploadedByEmail))
#     conn.commit()
#     sql_error = """UPDATE [jobPython]
#     SET [jobCompleted] = NULL, [jobErrDetail]= 'Update Database ECL'
#     WHERE [jobName] = 'ECL';
#                 """
#     cursor.execute(sql_error)
#     conn.commit()
#     print(f"Update Database ECL Error: {e}")
#     sys.exit(f"Update Database ECL Error: {str(e)}")

#     #==============================================================================================

#     columns = ['aftd_id','result_file_name','processed_status_id','status_id']
#     data = [(documentId,"Not Applicable",'PY003','PY003')] #,36961,36961
#     download_error = pd.DataFrame(data,columns=columns)
    
#     # Assuming 'combine2' is a DataFrame
#     column_types1 = []
#     for col in download_error.columns:
#         # You can choose to map column types based on data types in the DataFrame, for example:
#         if download_error[col].dtype == 'object':  # String data type
#             column_types1.append(f"{col} VARCHAR(255)")
#         elif download_error[col].dtype == 'int64':  # Integer data type
#             column_types1.append(f"{col} INT")
#         elif download_error[col].dtype == 'float64':  # Float data type
#             column_types1.append(f"{col} FLOAT")
#         else:
#             column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

#     create_table_query_result = "CREATE TABLE A_download_error (" + ', '.join(column_types1) + ")"
#     cursor.execute(create_table_query_result)

#     for row in download_error.iterrows():
#         sql_result = "INSERT INTO A_download_error({}) VALUES ({})".format(','.join(download_error.columns), ','.join(['?']*len(download_error.columns)))
#         cursor.execute(sql_result, tuple(row[1]))
#     conn.commit()

#     cursor.execute("""MERGE INTO account_finance_transaction_documents AS target 
#                     USING A_download_error AS source
#                     ON target.aftd_id = source.aftd_id
#                     WHEN MATCHED THEN 
#                         UPDATE SET target.result_file_name = source.result_file_name,
#                         target.processed_status_id = (select param_id from param_system_param where param_code=source.processed_status_id),
#                         target.status_id = (select param_id from param_system_param where param_code=source.status_id);    
#     """)
#     conn.commit() 
#     cursor.execute("drop table A_download_error")
#     conn.commit() 

