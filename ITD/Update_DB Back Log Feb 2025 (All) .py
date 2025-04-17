import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

Test_jan = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\02. MIS Validation\\To update in db template.xlsx", sheet_name = "Feb25 P13")
#tukar tarikh

Test_jan.columns = Test_jan.columns.str.replace("\n","")
Test_jan.columns = Test_jan.columns.str.replace(" ","_")
Test_jan.columns = Test_jan.columns.str.replace(".","")
Test_jan.columns = Test_jan.columns.str.replace(")","")
Test_jan.columns = Test_jan.columns.str.replace("(","")
Test_jan.columns = Test_jan.columns.str.replace("&","n")
Test_jan.columns = Test_jan.columns.str.replace("/","_")
Test_jan.columns = Test_jan.columns.str.replace("`","")
Test_jan.columns = Test_jan.columns.str.replace("'","")


Test_jan["EXIM_Account_No"] = Test_jan["EXIM_Account_No"].str.replace("-","")

Test_jan.fillna(0, inplace=True)

Test_jan.head(1)

#buat 
#Total Loans Outstanding CCRIS (Facility Currency)
#Total Loans Outstanding CCRIS (MYR)
#26,40,48,42,44,46

Test_jan['Total_Loans_Outstanding_CCRIS_FC'] = Test_jan['Cost_Principal_Outstanding_Facility_Currency'] + Test_jan['Cumulative_Accrued_Profit_Interest_Facility_Currency'] + Test_jan['Other_Charges_Facility_Currency'] + Test_jan['Penalty_Tawidh_Facility_Currency'] + Test_jan['_Tawidh_Compensation__Facility_Currency'] + Test_jan['Income_Interest_in_Suspense_Facility_Currency']
Test_jan['Total_Loans_Outstanding_CCRIS_MYR'] = Test_jan['Cost_Principal_Outstanding_MYR'] + Test_jan['Cumulative_Accrued_Profit_Interest_MYR'] + Test_jan['Other_Charges_MYR'] + Test_jan['Penalty_Tawidh_MYR'] + Test_jan['_Tawidh_Compensation__MYR'] + Test_jan['Income_Interest_in_Suspense_MYR']


Test_jan1 = Test_jan[['EXIM_Account_No',
                      'Amount_Approved___Facility_Limit_Facility_Currency',
                      'Amount_Approved___Facility_Limit_MYR',
                      'Cost_Principal_Outstanding_Facility_Currency',
                      'Cost_Principal_Outstanding_MYR',
                      'Contingent_Liability_Letter_of_Credit_Facility_Currency',
                      'Contingent_Liability_Letter_of_Credit_MYR',
                      'Contingent_Liability_Facility_Currency',
                      'Contingent_Liability_MYR',
                      'Account_Receivables_Past_Due_Claims_Facility_Currency',
                      'Account_Receivable_Past_Due_Claims_MYR',
                      'Total_Banking_Exposure_Facility_Currency',
                      'Total_Banking_Exposure_MYR',
                      'Accrued_Profit_Interest_of_the_month_Facility_Currency',
                      'Accrued_Profit_Interest_of_the_month_MYR',
                      'Modification_of_Loss_Facility_Currency',
                      'Modification_of_Loss_MYR',
                      'Cumulative_Accrued_Profit_Interest_Facility_Currency',
                      'Cumulative_Accrued_Profit_Interest_MYR',
                      'Penalty_Tawidh_Facility_Currency',
                      'Penalty_Tawidh_MYR',
                      '_Tawidh_Compensation__Facility_Currency',
                      '_Tawidh_Compensation__MYR',
                      'Income_Interest_in_Suspense_Facility_Currency',
                      'Income_Interest_in_Suspense_MYR',
                      'Other_Charges_Facility_Currency',
                      'Other_Charges_MYR',
                      'Total_Loans_Outstanding_CCRIS_FC',
                      'Total_Loans_Outstanding_CCRIS_MYR',
                      'Total_Loans_Outstanding_Facility_Currency',
                      'Total_Loans_Outstanding_MYR',
                      'Expected_Credit_Loss_ECL_LAF_Facility_Currency',
                      'Expected_Credit_Loss_LAF_ECL_MYR',
                      'Expected_Credit_Loss_CnC_ECL_Facility_Currency',
                      'Expected_Credit_Loss_CnC_ECL_MYR',
                      'Unutilised_Undrawn_Amount_Facility_Currency',
                      'Unutilised_Undrawn_Amount_MYR',
                      'Disbursement_Drawdown_Facility_Currency',
                      'Disbursement_Drawdown_MYR',
                      'Cumulative_Disbursement_Drawdown_Facility_Currency',
                      'Cumulative_Disbursement_Drawdown_MYR',
                      'Cost_Payment_Principal_Repayment_Facility_Currency',
                      'Cost_Payment_Principal_Repayment_MYR',
                      'Cumulative_Cost_Payment_Principal_Repayment_Facility_Currency',
                      'Cumulative_Cost_Payment_Principal_Repayment_MYR',
                      'Profit_Payment_Interest_Repayment_Facility_Currency',
                      'Profit_Payment_Interest_Repayment_MYR',
                      'Cumulative_Profit_Payment_Interest_Repayment_Facility_Currency',
                      'Cumulative_Profit_Payment_Interest_Repayment_MYR',
                      'Tawidh_Payment_Penalty_Repayment_Facility_Currency',
                      'Tawidh_Payment_Penalty_Repayment__MYR',
                      'Cumulative_Tawidh_Payment_Penalty_Repayment_Facility_Currency',
                      'Cumulative_Tawidh_Payment_Penalty_Repayment__MYR',
                      'Other_Charges_Payment_Facility_Currency',
                      'Other_Charges_Payment_MYR',
                      'Cumulative_Other_Charges_Payment_Facility_Currency',
                      'Cumulative_Other_Charges_Payment_MYR']]

#tukar
Test_jan1['Position_as_At'] = '2025-02-28'

Test_jan1.shape

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.20.1.19,1455;'
    'DATABASE=mis_db_prod;'
    'UID=mis_admin;'
    'PWD=Exim1234;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)
#    'SERVER=10.32.1.51,1455;' UAT
#    'DATABASE=mis_db_prod_backup_2024_04_02;'

#    'SERVER=10.20.1.19,1455;' PROD
#    'DATABASE=mis_db_prod;'

cursor = conn.cursor()

# Assuming 'Test_jan1' is a DataFrame
column_types = []
for col in Test_jan1.columns:
    # You can choose to map column types based on data types in the DataFrame, for example:
    if Test_jan1[col].dtype == 'object':  # String data type
        column_types.append(f"{col} VARCHAR(255)")
    elif Test_jan1[col].dtype == 'int64':  # Integer data type
        column_types.append(f"{col} INT")
    elif Test_jan1[col].dtype == 'float64':  # Float data type
        column_types.append(f"{col} FLOAT")
    else:
        column_types.append(f"{col} VARCHAR(255)")  # Default type for others

# Generate the CREATE TABLE statement
create_table_query = "CREATE TABLE A_MYR (" + ', '.join(column_types) + ")"
# Execute the query
cursor.execute(create_table_query)

for row in Test_jan1.iterrows():
    sql = "INSERT INTO A_MYR({}) VALUES ({})".format(','.join(Test_jan1.columns), ','.join(['?']*len(Test_jan1.columns)))
    cursor.execute(sql, tuple(row[1]))
conn.commit()

cursor.execute("""MERGE INTO dbase_account_hist AS target 
USING A_MYR AS source
ON target.facility_exim_account_num = source.EXIM_Account_No
WHEN MATCHED AND target.position_as_at = source.Position_as_At THEN
    UPDATE SET target.facility_amount_approved = source.Amount_Approved___Facility_Limit_Facility_Currency,
             target.facility_amount_approved_myr = source.Amount_Approved___Facility_Limit_MYR,
             target.facility_amount_outstanding = source.Cost_Principal_Outstanding_Facility_Currency,
             target.acc_principal_amount_outstanding = source.Cost_Principal_Outstanding_MYR,
             target.acc_contingent_liability_letter_credit_fc  = source.Contingent_Liability_Letter_of_Credit_Facility_Currency,
             target.acc_contingent_liability_letter_credit_myr = source.Contingent_Liability_Letter_of_Credit_MYR,
             target.acc_contingent_liability_ori = source.Contingent_Liability_Facility_Currency,
             target.acc_contingent_liability_myr = source.Contingent_Liability_MYR,
             target.acc_receivables_past_due_claim_fc = source.Account_Receivables_Past_Due_Claims_Facility_Currency,
             target.acc_receivable_past_due_claim_myr = source.Account_Receivable_Past_Due_Claims_MYR,
             target.acc_total_banking_exposure_fc = source.Total_Banking_Exposure_Facility_Currency,
             target.acc_total_banking_exposure_myr = source.Total_Banking_Exposure_MYR,
             target.acc_accrued_interest_month_fc = source.Accrued_Profit_Interest_of_the_month_Facility_Currency,
             target.acc_accrued_interest_month_myr = source.Accrued_Profit_Interest_of_the_month_MYR,
             target.modification_of_loss_fc = source.Modification_of_Loss_Facility_Currency,
             target.modification_of_loss_myr = source.Modification_of_Loss_MYR,
             target.acc_accurate_interest = source.Cumulative_Accrued_Profit_Interest_Facility_Currency,
             target.acc_accrued_interest_myr = source.Cumulative_Accrued_Profit_Interest_MYR,
             target.acc_penalty = source.Penalty_Tawidh_Facility_Currency,
             target.acc_penalty_myr = source.Penalty_Tawidh_MYR,
             target.acc_penalty_compensation_fc = source._Tawidh_Compensation__Facility_Currency,
             target.acc_penalty_compensation_myr = source._Tawidh_Compensation__MYR,
             target.acc_suspended_interest = source.Income_Interest_in_Suspense_Facility_Currency,
             target.acc_interest_suspense_myr = source.Income_Interest_in_Suspense_MYR,
             target.acc_other_charges = source.Other_Charges_Facility_Currency,
             target.acc_other_charges_myr = source.Other_Charges_MYR,
             target.total_loans_outstanding_fc = source.Total_Loans_Outstanding_CCRIS_FC,
             target.total_loans_outstanding_myr = source.Total_Loans_Outstanding_CCRIS_MYR,
             target.acc_balance_outstanding_audited_fc = source.Total_Loans_Outstanding_Facility_Currency,
             target.acc_balance_outstanding_audited_myr = source.Total_Loans_Outstanding_MYR,
             target.acc_credit_loss_laf_ecl = source.Expected_Credit_Loss_ECL_LAF_Facility_Currency,
             target.acc_credit_loss_laf_ecl_myr = source.Expected_Credit_Loss_LAF_ECL_MYR,
             target.acc_credit_loss_cnc_ecl = source.Expected_Credit_Loss_CnC_ECL_Facility_Currency,
             target.acc_credit_loss_cnc_ecl_myr = source.Expected_Credit_Loss_CnC_ECL_MYR,
             target.acc_undrawn_amount_banking_ori = source.Unutilised_Undrawn_Amount_Facility_Currency,
             target.acc_undrawn_amount_myr = source.Unutilised_Undrawn_Amount_MYR,
             target.acc_drawdown_fc = source.Disbursement_Drawdown_Facility_Currency,
             target.acc_drawdown_myr = source.Disbursement_Drawdown_MYR,
             target.acc_cumulative_drawdown = source.Cumulative_Disbursement_Drawdown_Facility_Currency,
             target.acc_cumulative_drawdown_myr = source.Cumulative_Disbursement_Drawdown_MYR,
             target.acc_repayment_fc = source.Cost_Payment_Principal_Repayment_Facility_Currency,
             target.acc_repayment_myr = source.Cost_Payment_Principal_Repayment_MYR,
             target.acc_cumulative_repayment = source.Cumulative_Cost_Payment_Principal_Repayment_Facility_Currency,
             target.acc_cumulative_repayment_myr = source.Cumulative_Cost_Payment_Principal_Repayment_MYR,
             target.acc_interest_repayment_fc = source.Profit_Payment_Interest_Repayment_Facility_Currency,
             target.acc_interest_repayment_myr = source.Profit_Payment_Interest_Repayment_MYR,
             target.acc_cumulative_interest_repayment_fc = source.Cumulative_Profit_Payment_Interest_Repayment_Facility_Currency,
             target.acc_cumulative_interest_repayment_myr = source.Cumulative_Profit_Payment_Interest_Repayment_MYR,
             target.penalty_repayment = source.Tawidh_Payment_Penalty_Repayment_Facility_Currency,
             target.penalty_repayment_myr = source.Tawidh_Payment_Penalty_Repayment__MYR,
             target.cumulative_penalty = source.Cumulative_Tawidh_Payment_Penalty_Repayment_Facility_Currency,
             target.cumulative_penalty_myr = source.Cumulative_Tawidh_Payment_Penalty_Repayment__MYR,
             target.other_charges_payment = source.Other_Charges_Payment_Facility_Currency,
             target.other_charges_payment_myr = source.Other_Charges_Payment_MYR,
             target.cumulative_other_charges_payment = source.Cumulative_Other_Charges_Payment_Facility_Currency,
             target.cumulative_other_charges_payment_myr = source.Cumulative_Other_Charges_Payment_MYR;
""")
conn.commit() 

cursor.execute("drop table A_MYR")
conn.commit() 

conn.close()