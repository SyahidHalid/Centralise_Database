# python CCRIS_Template.py 1, "a", "CCRIS Template", "Pending Processing", "0", "syahidhalid@exim.com.my","2025-07-31"

#   reportingDate = '2025-09-30'
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
    #   Active_before.head(1)
    #   Active_before.shape
    def format_18_digit(val: str) -> str:
        val = str(val)
        if len(val) == 18 and val.isdigit():
            return f'{val[0:4]}-{val[4:9]}-{val[9:12]}-{val[12:16]}-{val[16:18]}'
        return val

    Active_before['No.'] = range(1, len(Active_before) + 1)

    Active_before['facility_exim_account_num_new'] = Active_before['facility_exim_account_num'].map(format_18_digit)

    Active_before['Ownership'] = "0"
    Active_before['Officer in Charge'] = "0"
    Active_before['Restructured / Rescheduled (Y/N)'] = "0" ##################
    Active_before['PF'] = "0"
    Active_before['LGD'] = "0"
    #Active_before['Column1'] = "" ##################
    Active_before['Risk Category'] = "0"
    Active_before['Prudential Limit (%) '] = "0"
    Active_before["EXIM's Shareholder Fund as at"] = "0"
    Active_before["EXIM's Shareholder Fund as at  (MYR)"] = "0"
    Active_before['Single Customer Exposure Limit (SCEL)(MYR)'] = "0"
    Active_before['Percentage of Total Banking Exposure(MYR) to SCEL (MYR)'] = "0"
    Active_before['Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)'] = "0"
    Active_before['Risk Analyst'] = "0"
    Active_before['SME Commercial Corporate'] = "0"
    Active_before['EXIM Main Sector'] = "0"
    Active_before['Industry (Risk)'] = "0"
    Active_before['Industry Classification'] = "0"

    LDB2 = Active_before[['No.',"cif_number",
    "facility_exim_account_num_new",
    "facility_application_sys_code_desc",
    "facility_ccris_master_account_num",
    "facility_ccris_sub_account_num",
    "finance_sap_number",
    "cif_company_group",
    "cif_name",
    "acc_relationship_manager_rm",
    "acc_banking_team",
    "Ownership",
    "Officer in Charge",
    "syndicated_deal_desc",
    "acc_nature_acc_desc",
    "facility_type_id_desc",
    "facility_ccy_id_desc",
    "financing_type_desc",
    "shariah_concept_desc",
    "acc_status_desc",
    "ca_post_approval_stage_desc",
    "date_ready_utilization",
    "Restructured / Rescheduled (Y/N)",
    "facility_amount_approved",
    "facility_amount_approved_myr",
    "facility_amount_outstanding",
    "acc_principal_amount_outstanding",
    "acc_contingent_liability_letter_credit_fc",
    "acc_contingent_liability_letter_credit_myr",
    "acc_contingent_liability_ori",
    "acc_contingent_liability_myr",
    "acc_receivables_past_due_claim_fc",
    "acc_receivable_past_due_claim_myr",
    "acc_total_banking_exposure_fc",
    "acc_total_banking_exposure_myr",
    "acc_accrued_interest_month_fc",
    "acc_accrued_interest_month_myr",
    "modification_of_loss_fc",
    "modification_of_loss_myr",
    "acc_accurate_interest",
    "acc_accrued_interest_myr",
    "acc_penalty",
    "acc_penalty_myr",
    "acc_suspended_interest",
    "acc_interest_suspense_myr",
    "acc_other_charges",
    "acc_other_charges_myr",
    "acc_balance_outstanding_fc",
    "acc_balance_outstanding_myr",
    "acc_credit_loss_laf_ecl",
    "acc_credit_loss_laf_ecl_myr",
    "acc_disbursement_status_desc",
    "acc_undrawn_amount_banking_ori",
    "acc_undrawn_amount_myr",
    "acc_drawdown_fc",
    "acc_drawdown_myr",
    "acc_cumulative_drawdown",
    "acc_cumulative_drawdown_myr",
    "acc_repayment_fc",
    "acc_repayment_myr",
    "acc_cumulative_repayment",
    "acc_cumulative_repayment_myr",
    "acc_interest_repayment_fc",
    "acc_interest_repayment_myr",
    "acc_cumulative_interest_repayment_fc",
    "acc_cumulative_interest_repayment_myr",
    "penalty_repayment",
    "penalty_repayment_myr",
    "cumulative_penalty",
    "cumulative_penalty_myr",
    "other_charges_payment",
    "other_charges_payment_myr",
    "cumulative_other_charges_payment",
    "cumulative_other_charges_payment_myr",
    "acc_rating_origination",
    "acc_PD",
    "PF",
    "LGD",
    "crms_obligator_risk_rating",
    "pd_percent",
    "lgd_percent",
    #"Column1",
    "Risk Category",
    "Prudential Limit (%) ",
    "EXIM's Shareholder Fund as at",
    "EXIM's Shareholder Fund as at  (MYR)",
    "Single Customer Exposure Limit (SCEL)(MYR)",
    "Percentage of Total Banking Exposure(MYR) to SCEL (MYR)",
    "Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)",
    "Risk Analyst",
    "acc_MFRS9_staging_desc",
    "bnm_main_sector_desc",
    "bnm_sub_sector_desc",
    "EXIM Main Sector",
    "Industry (Risk)",
    "Industry Classification",
    "purpose_financing",
    "approved_date",
    "approval_authority_desc",
    "acc_lo_issuance_date",
    "acc_date_lo_acceptance",
    "acc_first_disbursement_date",
    "acc_first_repayment_date",
    "acc_availability_period",
    "acc_facility_agreement_date",
    "acc_review_date",
    "acc_watchlist_review_date_approval",
    "acc_maturity_expired_date",
    "acc_grace_period",
    "moratorium_period_month",
    "moratorium_start_date",
    "fund_type_desc",
    "acc_tenure",
    "acc_payment_frequency_interest",
    "acc_payment_frequency_principal",
    "acc_effective_cost_borrowings_desc",
    "acc_margin",
    "acc_average_interest_rate",
    "acc_tadwih_compensation",
    "cif_operation_country_desc",
    "facility_country_id_desc",
    "acc_country_rating",
    "acc_region_desc",
    "market_type_desc",
    "classification_cust_type_desc",
    "cif_cust_type_desc",
    "classification_residency_status_desc",
    "cif_residency_status_desc",
    "cif_corporate_type_desc",
    "SME Commercial Corporate",
    "cif_corporate_status_desc",
    "justification_corporate_status_desc",
    "rrtag_desc",
    "dateapp_date",
    "dateapp_effectivedate",
    "dateapp_reason",
    "frequency_rr",
    "acc_date_overdue",
    "acc_overdue_days",
    "int_month_in_arrears",
    "acc_overdue_ori",
    "acc_overdue_amount_myr",
    "acc_watchlist_date",
    "acc_watchlist_reason",
    "acc_date_delist_watchlist",
    "acc_date_impaired",
    "acc_reason_impairment",
    "acc_partial_writeoff_date",
    "acc_writeoff_date",
    "acc_cancel_fulltsettle_date",
    "position_as_at",
    "acc_credit_loss_cnc_ecl","acc_credit_loss_cnc_ecl_myr"]]

    # LDB2.shape
    # LDB2['finance_sap_number'].value_counts()
    # LDB2.iloc[np.where(LDB2['finance_sap_number'].isin(['BG-I','BG','500724']))][["acc_credit_loss_cnc_ecl","acc_credit_loss_cnc_ecl_myr"]]

    LDB2.loc[(LDB2['finance_sap_number'].isin(['BG-I','BG','500724'])),'acc_credit_loss_cnc_ecl'] = 0
    LDB2.loc[(LDB2['finance_sap_number'].isin(['BG-I','BG','500724'])),'acc_credit_loss_cnc_ecl_myr'] = 0

    LDB2['acc_credit_loss_laf_ecl_new'] = LDB2['acc_credit_loss_laf_ecl'].fillna(0) + LDB2['acc_credit_loss_cnc_ecl'].fillna(0)
    LDB2['acc_credit_loss_laf_ecl_myr_new'] = LDB2['acc_credit_loss_laf_ecl_myr'].fillna(0) + LDB2['acc_credit_loss_cnc_ecl_myr'].fillna(0)

    LDB3 = LDB2.drop(['acc_credit_loss_cnc_ecl','acc_credit_loss_laf_ecl_myr'],axis=1)
    
    LDB3.fillna(0,inplace=True)

    #   LDB3.shape
    LDB4 = LDB3[['No.',"cif_number",
    "facility_exim_account_num_new",
    "facility_application_sys_code_desc",
    "facility_ccris_master_account_num",
    "facility_ccris_sub_account_num",
    "finance_sap_number",
    "cif_company_group",
    "cif_name",
    "acc_relationship_manager_rm",
    "acc_banking_team",
    "Ownership",
    "Officer in Charge",
    "syndicated_deal_desc",
    "acc_nature_acc_desc",
    "facility_type_id_desc",
    "facility_ccy_id_desc",
    "financing_type_desc",
    "shariah_concept_desc",
    "acc_status_desc",
    "ca_post_approval_stage_desc",
    "date_ready_utilization",
    "Restructured / Rescheduled (Y/N)",
    "facility_amount_approved",
    "facility_amount_approved_myr",
    "facility_amount_outstanding",
    "acc_principal_amount_outstanding",
    "acc_contingent_liability_letter_credit_fc",
    "acc_contingent_liability_letter_credit_myr",
    "acc_contingent_liability_ori",
    "acc_contingent_liability_myr",
    "acc_receivables_past_due_claim_fc",
    "acc_receivable_past_due_claim_myr",
    "acc_total_banking_exposure_fc",
    "acc_total_banking_exposure_myr",
    "acc_accrued_interest_month_fc",
    "acc_accrued_interest_month_myr",
    "modification_of_loss_fc",
    "modification_of_loss_myr",
    "acc_accurate_interest",
    "acc_accrued_interest_myr",
    "acc_penalty",
    "acc_penalty_myr",
    "acc_suspended_interest",
    "acc_interest_suspense_myr",
    "acc_other_charges",
    "acc_other_charges_myr",
    "acc_balance_outstanding_fc",
    "acc_balance_outstanding_myr",
    "acc_credit_loss_laf_ecl_new",
    "acc_credit_loss_laf_ecl_myr_new",
    "acc_disbursement_status_desc",
    "acc_undrawn_amount_banking_ori",
    "acc_undrawn_amount_myr",
    "acc_drawdown_fc",
    "acc_drawdown_myr",
    "acc_cumulative_drawdown",
    "acc_cumulative_drawdown_myr",
    "acc_repayment_fc",
    "acc_repayment_myr",
    "acc_cumulative_repayment",
    "acc_cumulative_repayment_myr",
    "acc_interest_repayment_fc",
    "acc_interest_repayment_myr",
    "acc_cumulative_interest_repayment_fc",
    "acc_cumulative_interest_repayment_myr",
    "penalty_repayment",
    "penalty_repayment_myr",
    "cumulative_penalty",
    "cumulative_penalty_myr",
    "other_charges_payment",
    "other_charges_payment_myr",
    "cumulative_other_charges_payment",
    "cumulative_other_charges_payment_myr",
    "acc_rating_origination",
    "acc_PD",
    "PF",
    "LGD",
    "crms_obligator_risk_rating",
    "pd_percent",
    "lgd_percent",
    #"Column1",
    "Risk Category",
    "Prudential Limit (%) ",
    "EXIM's Shareholder Fund as at",
    "EXIM's Shareholder Fund as at  (MYR)",
    "Single Customer Exposure Limit (SCEL)(MYR)",
    "Percentage of Total Banking Exposure(MYR) to SCEL (MYR)",
    "Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)",
    "Risk Analyst",
    "acc_MFRS9_staging_desc",
    "bnm_main_sector_desc",
    "bnm_sub_sector_desc",
    "EXIM Main Sector",
    "Industry (Risk)",
    "Industry Classification",
    "purpose_financing",
    "approved_date",
    "approval_authority_desc",
    "acc_lo_issuance_date",
    "acc_date_lo_acceptance",
    "acc_first_disbursement_date",
    "acc_first_repayment_date",
    "acc_availability_period",
    "acc_facility_agreement_date",
    "acc_review_date",
    "acc_watchlist_review_date_approval",
    "acc_maturity_expired_date",
    "acc_grace_period",
    "moratorium_period_month",
    "moratorium_start_date",
    "fund_type_desc",
    "acc_tenure",
    "acc_payment_frequency_interest",
    "acc_payment_frequency_principal",
    "acc_effective_cost_borrowings_desc",
    "acc_margin",
    "acc_average_interest_rate",
    "acc_tadwih_compensation",
    "cif_operation_country_desc",
    "facility_country_id_desc",
    "acc_country_rating",
    "acc_region_desc",
    "market_type_desc",
    "classification_cust_type_desc",
    "cif_cust_type_desc",
    "classification_residency_status_desc",
    "cif_residency_status_desc",
    "cif_corporate_type_desc",
    "SME Commercial Corporate",
    "cif_corporate_status_desc",
    "justification_corporate_status_desc",
    "rrtag_desc",
    "dateapp_date",
    "dateapp_effectivedate",
    "dateapp_reason",
    "frequency_rr",
    "acc_date_overdue",
    "acc_overdue_days",
    "int_month_in_arrears",
    "acc_overdue_ori",
    "acc_overdue_amount_myr",
    "acc_watchlist_date",
    "acc_watchlist_reason",
    "acc_date_delist_watchlist",
    "acc_date_impaired",
    "acc_reason_impairment",
    "acc_partial_writeoff_date",
    "acc_writeoff_date",
    "acc_cancel_fulltsettle_date",
    "position_as_at"]].rename(columns={"cif_number":"CIF Number",
                                     "facility_exim_account_num_new":"EXIM Account No.",
                                     "facility_application_sys_code_desc":"Application System Code",
                                     "facility_ccris_master_account_num":"CCRIS Master Account Number",
                                     "facility_ccris_sub_account_num":"CCRIS Sub Account Number",
                                     "finance_sap_number":"Finance(SAP) Number",
                                     "cif_company_group":"Company Group",
                                     "cif_name":"Customer Name",
                                     "acc_relationship_manager_rm":"Relationship Manager (RM)",
                                     "acc_banking_team":"Banking Team",
                                     "Ownership":"Ownership",
                                     "Officer in Charge":"Officer in Charge",
                                     "syndicated_deal_desc":"Syndicated / Club Deal",
                                     "acc_nature_acc_desc":"Nature of Account",
                                     "facility_type_id_desc":"Facility",
                                     "facility_ccy_id_desc":"Facility Currency",
                                     "financing_type_desc":"Type of Financing",
                                     "shariah_concept_desc":"Shariah Contract / Concept",
                                     "acc_status_desc":"Status",
                                     "ca_post_approval_stage_desc":"Post Approval Stage",
                                     "date_ready_utilization":"Date of Ready for Utilization (RU)",
                                     "Restructured / Rescheduled (Y/N)":"Restructured / Rescheduled (Y/N)", #
                                     "facility_amount_approved":"Amount Approved / Facility Limit (Facility Currency)",
                                     "facility_amount_approved_myr":"Amount Approved / Facility Limit (MYR)",
                                     "facility_amount_outstanding":"Cost/Principal Outstanding (Facility Currency)",
                                     "acc_principal_amount_outstanding":"Cost/Principal Outstanding (MYR)",
                                     "acc_contingent_liability_letter_credit_fc":"Contingent Liability Letter of Credit (Facility Currency)",
                                     "acc_contingent_liability_letter_credit_myr":"Contingent Liability Letter of Credit (MYR)",
                                     "acc_contingent_liability_ori":"Contingent Liability (Facility Currency)",
                                     "acc_contingent_liability_myr":"Contingent Liability (MYR)",
                                     "acc_receivables_past_due_claim_fc":"Account Receivables/Past Due Claims (Facility Currency)",
                                     "acc_receivable_past_due_claim_myr":"Account Receivable/Past Due Claims (MYR)",
                                     "acc_total_banking_exposure_fc":"Total Banking Exposure  Facility Currency)", #
                                     "acc_total_banking_exposure_myr":"Total Banking Exposure (MYR)",
                                     "acc_accrued_interest_month_fc":"Accrued Profit/ Interest of the month  (Facility Currency)", #
                                     "acc_accrued_interest_month_myr":"Accrued Profit/Interest of the month (MYR)", #
                                     "modification_of_loss_fc":"Modification of Loss (Facility Currency)",
                                     "modification_of_loss_myr":"Modification of Loss (MYR)",
                                     "acc_accurate_interest":"Cumulative Accrued Profit/Interest (Facility Currency)",
                                     "acc_accrued_interest_myr":"Cumulative Accrued Profit/Interest (MYR)",
                                     "acc_penalty":"Penalty/Ta`widh (Facility Currency)",
                                     "acc_penalty_myr":"Penalty/Ta`widh (MYR)",
                                     "acc_suspended_interest":"Income/Interest in Suspense (Facility Currency)",
                                     "acc_interest_suspense_myr":"Income/Interest in Suspense (MYR)",
                                     "acc_other_charges":"Other Charges (Facility Currency)",
                                     "acc_other_charges_myr":"Other Charges (MYR)",
                                     "acc_balance_outstanding_fc":"Total Loans Outstanding (Facility Currency)",
                                     "acc_balance_outstanding_myr":"Total Loans Outstanding (MYR)",
                                     "acc_credit_loss_laf_ecl_new":"Expected Credit Loss (ECL) LAF (Facility Currency)",
                                     "acc_credit_loss_laf_ecl_myr_new":"Expected Credit Loss LAF (ECL) (MYR)",
                                     "acc_disbursement_status_desc":"Disbursement/ Drawdown Status", #
                                     "acc_undrawn_amount_banking_ori":"Unutilised/ Undrawn Amount (Facility Currency)", #
                                     "acc_undrawn_amount_myr":"Unutilised/ Undrawn Amount (MYR)", #
                                     "acc_drawdown_fc":"Disbursement/ Drawdown  (Facility Currency)", #
                                     "acc_drawdown_myr":"Disbursement/Drawdown (MYR)",
                                     "acc_cumulative_drawdown":"Cumulative Disbursement/ Drawdown  (Facility Currency)", #
                                     "acc_cumulative_drawdown_myr":"Cumulative Disbursement/ Drawdown  (MYR)", #
                                     "acc_repayment_fc":"Cost Payment/ Principal Repayment  (Facility Currency)", #
                                     "acc_repayment_myr":"Cost Payment/ Principal Repayment  (MYR)", #
                                     "acc_cumulative_repayment":"Cumulative  Cost Payment/ Principal Repayment (Facility Currency)", #
                                     "acc_cumulative_repayment_myr":"Cumulative  Cost Payment/ Principal Repayment  (MYR)", #
                                     "acc_interest_repayment_fc":"Profit Payment/ Interest Repayment (Facility Currency)", #
                                     "acc_interest_repayment_myr":"Profit Payment/ Interest Repayment  (MYR)", #
                                     "acc_cumulative_interest_repayment_fc":"Cumulative  Profit Payment/ Interest Repayment (Facility Currency)", #
                                     "acc_cumulative_interest_repayment_myr":"Cumulative  Profit Payment/ Interest Repayment  (MYR)",
                                     "penalty_repayment":"Ta`widh Payment/ Penalty Repayment (Facility Currency)",
                                     "penalty_repayment_myr":"Ta`widh Payment/ Penalty Repayment   (MYR)", #
                                     "cumulative_penalty":"Cumulative  Ta`widh Payment/ Penalty Repayment (Facility Currency)", #
                                     "cumulative_penalty_myr":"Cumulative  Ta`widh Payment/ Penalty Repayment   (MYR)", #
                                     "other_charges_payment":"Other Charges Payment (Facility Currency)",
                                     "other_charges_payment_myr":"Other Charges Payment (MYR)",
                                     "cumulative_other_charges_payment":"Cumulative Other Charges Payment (Facility Currency)",
                                     "cumulative_other_charges_payment_myr":"Cumulative Other Charges Payment (MYR)",
                                     "acc_rating_origination":"Rating at Origination",
                                     "acc_PD":"PD",
                                     "PF":"PF",
                                     "LGD":"LGD",
                                     "crms_obligator_risk_rating":"CRMS Obligor Risk Rating",
                                     "pd_percent":"PD (%)",
                                     "lgd_percent":"LGD (%)",
                                     "Risk Category":"Risk Category",
                                     "Prudential Limit (%) ":"Prudential Limit (%) ",
                                     "EXIM's Shareholder Fund as at":"EXIM's Shareholder Fund as at",
                                     "EXIM's Shareholder Fund as at  (MYR)":"EXIM's Shareholder Fund as at  (MYR)",
                                     "Single Customer Exposure Limit (SCEL)(MYR)":"Single Customer Exposure Limit (SCEL)(MYR)",
                                     "Percentage of Total Banking Exposure(MYR) to SCEL (MYR)":"Percentage of Total Banking Exposure(MYR) to SCEL (MYR)",
                                     "Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)":"Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)",
                                     "Risk Analyst":"Risk Analyst",
                                     "acc_MFRS9_staging_desc":"MFRS9 Staging",
                                     "bnm_main_sector_desc":"BNM Main Sector",
                                     "bnm_sub_sector_desc":"BNM Sub Sector",
                                     "EXIM Main Sector":"EXIM Main Sector",
                                     "Industry (Risk)":"Industry (Risk)",
                                     "Industry Classification":"Industry Classification",
                                     "purpose_financing":"Purpose of Financing",
                                     "approved_date":"Date Approved at Origination",
                                     "approval_authority_desc":"Approval Authority",
                                     "acc_lo_issuance_date":"LO issuance Date",
                                     "acc_date_lo_acceptance":"Date of LO Acceptance",
                                     "acc_first_disbursement_date":"1st Disbursement/Drawdown Date",
                                     "acc_first_repayment_date":"1st Payment/ Repayment Date", #
                                     "acc_availability_period":"Expiry of Availability Period",
                                     "acc_facility_agreement_date":"Facility Agreement Date",
                                     "acc_review_date":"Annual Review Date",
                                     "acc_watchlist_review_date_approval":"Watchlist Review Date",
                                     "acc_maturity_expired_date":"Maturity/Expired Date",
                                     "acc_grace_period":"Grace Period (Month)",
                                     "moratorium_period_month":"Moratorium Period (Month) ",
                                     "moratorium_start_date":"Start Moratorium Date",
                                     "fund_type_desc":"Fund Type",
                                     "acc_tenure":"Tenure (Month)",
                                     "acc_payment_frequency_interest":"Payment/Repayment Frequency (Profit/Interest)",
                                     "acc_payment_frequency_principal":"Payment/Repayment Frequency (Cost/Principal)",
                                     "acc_effective_cost_borrowings_desc":"Effective cost of borrowings",
                                     "acc_margin":"Profit/Interest Margin",
                                     "acc_average_interest_rate":"Effective Interest Rate (EIR)",
                                     "acc_tadwih_compensation":"Ta`widh Compensation/Penalty Rate",
                                     "cif_operation_country_desc":"Operation Country",
                                     "facility_country_id_desc":"Country Exposure",
                                     "acc_country_rating":"Country Rating",
                                     "acc_region_desc":"Region",
                                     "market_type_desc":"Market Type",
                                     "classification_cust_type_desc":"Classification of Entity / Customer Type",
                                     "cif_cust_type_desc":"Entity / Customer Type",
                                     "classification_residency_status_desc":"Classification of Residency Status",
                                     "cif_residency_status_desc":"Residency Status",
                                     "cif_corporate_type_desc":"Corporate Type",
                                     "SME Commercial Corporate":"SME Commercial Corporate",
                                     "cif_corporate_status_desc":"Corporate Status",
                                     "justification_corporate_status_desc":"Justification on Corporate Status ",
                                     "rrtag_desc":"Restructured / Rescheduled",
                                     "dateapp_date":"Date of Approval Restructured / Rescheduled",
                                     "dateapp_effectivedate":"Effective Date",
                                     "dateapp_reason":"Reason",
                                     "frequency_rr":"Frequency",
                                     "acc_date_overdue":"Date of Overdue",
                                     "acc_overdue_days":"Overdue (Days)",
                                     "int_month_in_arrears":"Month in Arrears",
                                     "acc_overdue_ori":"Overdue Amount (Facility Currency)",
                                     "acc_overdue_amount_myr":"Overdue Amount (MYR)",
                                     "acc_watchlist_date":"Date Classified as Watchlist",
                                     "acc_watchlist_reason":"Watchlist Reason",
                                     "acc_date_delist_watchlist":"Date Declassified from Watchlist",
                                     "acc_date_impaired":"Date Impaired",
                                     "acc_reason_impairment":"Reason for Impairment",
                                     "acc_partial_writeoff_date":"Partial Write off Date",
                                     "acc_writeoff_date":"Write off Date",
                                     "acc_cancel_fulltsettle_date":"Cancellation Date/Fully Settled Date",
                                     "position_as_at":"Position as At"})
    
    #---------------------------------------------Details-------------------------------------------------------------
    
    # Extract
    # LDB4.head(1)
    # LDB4.shape
    convert_time = str(current_time).replace(":","-")

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