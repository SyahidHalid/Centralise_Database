
import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

LDB = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - FAD\\00. Loan Database\\Data Source\\202504\\Working\\Streamlit_04. Loan Database as at Apr 2025_Final v1.xlsx", sheet_name = "Loan Database", header=1)

MIS = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\02. MIS Validation\\Test 20250525 _apr2025 loanDatabaseReport_25052025_175249.xlsx", sheet_name = "Page 1", header=4)


LDB.head(1)
LDB.shape

MIS.head(1)
MIS.shape

MIS.rename(columns={"Ta'widh / Compensation / Penalty Rate":"Ta`widh Compensation/Penalty Rate",
                    "Interest/Profit Rate":"Effective cost of borrowings"}, inplace=True)

LDB["finance_sap_number"] = LDB["finance_sap_number"].astype(str)

LDB1 = LDB[["cif_number",
"facility_application_sys_code",
"facility_ccris_master_account_num",
"facility_ccris_sub_account_num",
"facility_exim_account_num",
"finance_sap_number",
"cif_company_group",
"cif_name",
"acc_status",
"dateapp_type",
"facility_type_id",
#N/A
"facility_ccy_id",
"facility_amount_approved",
"facility_amount_approved_myr",
#N/A
"facility_amount_outstanding",
"acc_principal_amount_outstanding",
"acc_contingent_liability_letter_credit_fc",
"acc_contingent_liability_letter_credit_myr",
"acc_contingent_liability_ori",
"acc_contingent_liability_myr",
"acc_receivables_past_due_claim_fc",
"acc_receivable_past_due_claim_myr",
"acc_accrued_interest_month_fc",
"acc_accrued_interest_month_myr",
"modification_of_loss_fc",
"modification_of_loss_myr",
"acc_accurate_interest",
"acc_accrued_interest_myr",
"acc_penalty",
"acc_penalty_myr",
"acc_penalty_compensation_fc",
"acc_penalty_compensation_myr",
"acc_suspended_interest",
"acc_interest_suspense_myr",
"acc_other_charges",
"acc_other_charges_myr",
#N/A
#N/A
"acc_balance_outstanding_audited_fc",
"acc_balance_outstanding_audited_myr",
"acc_total_banking_exposure_fc",
"acc_total_banking_exposure_myr",
"acc_disbursement_status",
"acc_credit_loss_laf_ecl",
"acc_credit_loss_laf_ecl_myr",
"acc_credit_loss_cnc_ecl",
"acc_credit_loss_cnc_ecl_myr",
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
"ca_post_approval_stage",
"date_ready_utilization",
"acc_first_disbursement_date",
"acc_first_repayment_date",
"acc_nature_acc",
"financing_type",
"shariah_concept",
"syndicated_deal",
"fund_type",
"incentive",
"program_lending",
"guarantee",
"purpose_financing",
#N/A
"approved_date",
"approval_authority",
"acc_lo_issuance_date",
"acc_date_lo_acceptance",
"acc_facility_agreement_date",
"acc_availability_period",
"acc_maturity_expired_date",
"acc_tenure",
"cif_operation_country",
"facility_country_id",
#N/A
"acc_country_rating",
"acc_region",
"market_type",
"classification_cust_type",
"cif_cust_type",
"classification_residency_status",
"classificationResidencyStatus",
"cif_residency_status",
"cif_corporate_type",
"cif_corporate_status",
"justification_corporate_status",
"ccpt_classification",
"acc_rating_origination",
"internal_credit_rating",
"crms_obligator_risk_rating",
"crms_cg_rating",
"pd_percent",
"lgd_percent",
"acc_MFRS9_staging",
"bnm_main_sector",
"bnm_sub_sector",
"oil_and_gas_desc",
"oil_and_gas_segmentation",
"acc_review_date",
"acc_watchlist_review_date_approval",
"acc_payment_frequency_interest",
"acc_payment_frequency_principal",
"acc_effective_cost_borrowings",
"acc_margin",
"acc_average_interest_rate",
"acc_tadwih_compensation",
"dateapp_date",
"dateapp_effectivedate",
"dateapp_reason",
"date_untagged_rr",
"justification_untagged",
"frequency_rr",
"acc_grace_period",
"moratorium_period_month",
"moratorium_start_date",
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
#N/A
#N/A
"acc_relationship_manager_rm",
"acc_banking_team",
"position_as_at"]]

MIS1 = MIS[["EXIM Account Number",
            "CIF Number",
            "Application System Code",
            "CCRIS Master Account Number",
            #"Unnamed: 5",
            "CCRIS Sub Account Number",
            #"EXIM Account Number", original pos
            "Finance SAP Number",
            "Company Group",
            "Customer Name",
            "Status of Account",
            "Application Type",
            "Facility",
            "BNM Facility Type Classification (CCRIS)",
            "Facility Currency",
            "Amount Approved / Facility Limit (Facility Currency)",
            "Amount Approved / Facility Limit (MYR)",
            "Financing Size",
            "Cost/Principal Outstanding (Facility Currency)",
            "Cost/Principal Outstanding (MYR)",
            "Contingent Liability Letter of Credit (Facility Currency)",
            "Contingent Liability Letter of Credit (MYR)",
            "Contingent Liability (Facility Currency)",
            "Contingent Liability (MYR)",
            "Account Receivables/Past Due Claims (Facility Currency)",
            "Account Receivable/Past Due Claims (MYR)",
            "Accrued Profit/Interest of the month(Facility Currency)",
            "Accrued Profit/Interest of the month(MYR)",
            "Modification of Loss (Facility Currency)",
            "Modification of Loss (MYR)",
            "Cumulative Accrued Profit/Interest (Facility Currency)",
            "Cumulative Accrued Profit/Interest (MYR)",
            "Penalty/Ta`widh (Facility Currency)",
            "Penalty/Ta`widh (MYR)",
            "Ta'widh (Compensation) (Facility Currency)",
            "Ta'widh (Compensation) (MYR)",
            "Income/Interest in Suspense (Facility Currency)",
            "Income/Interest in Suspense (MYR)",
            "Other Charges (Facility Currency)",
            "Other Charges (MYR)",
            "Total Loans Outstanding CCRIS (Facility Currency)",
            "Total Loans Outstanding CCRIS (MYR)",
            "Total Loans Outstanding FS (Facility Currency)",
            "Total Loans Outstanding FS (MYR)",
            "Total Banking Exposure (Facility Currency)",
            "Total Banking Exposure (MYR)",
            "Disbursement/Drawdown Status",
            "Expected Credit Loss (ECL) LAF (Facility Currency)",
            "Expected Credit Loss LAF (ECL) (MYR)",
            "Expected Credit Loss C&C (ECL) (Facility Currency)",
            "Expected Credit Loss C&C (ECL) (MYR)",
            "Unutilised/Undrawn Amount (Facility Currency)",
            "Unutilised/Undrawn Amount (MYR)",
            "Disbursement/Drawdown (Facility Currency)",
            "Disbursement/Drawdown (MYR)",
            "Cumulative Disbursement/Drawdown (Facility Currency)",
            "Cumulative Disbursement/Drawdown (MYR)",
            "Cost Payment/Principal Repayment (Facility Currency)",
            "Cost Payment/Principal Repayment (MYR)",
            "Cumulative Cost Payment/Principal Repayment (Facility ",
            "Cumulative Cost Payment/Principal Repayment (MYR)",
            "Profit Payment/Interest Repayment (Facility Currency)",
            "Profit Payment/Interest Repayment (MYR)",
            "Cumulative Profit Payment/Interest Repayment (Facility ",
            "Cumulative Profit Payment/Interest Repayment (MYR)",
            "Ta`widh Payment/Penalty Repayment (Facility Currency)",
            "Ta`widh Payment/Penalty Repayment (MYR)",
            "Cumulative Ta`widh Payment/Penalty Repayment (Facility ",
            "Cumulative Ta`widh Payment/Penalty Repayment (MYR)",
            "Other Charges Payment (Facility Currency)",
            "Other Charges Payment (MYR)",
            "Cumulative Other Charges Payment (Facility Currency)",
            "Cumulative Other Charges Payment (MYR)",
            "Post Approval Stage",
            "Date of Ready for Utilization (RU)",
            "1st Disbursement Date / 1st Drawdown Date",
            "1st Payment/Repayment Date",
            "Nature of Account",
            "Type of Financing",
            "Shariah Contract / Concept",
            "Syndicated / Club Deal",
            "Fund Type",
            "Incentive",
            "Programme",
            "Guarantee",
            "Purpose of Financing",
            "BNM Purpose of Loan",
            "Date Approved at Origination",
            "Approval Authority",
            "LO issuance Date",
            "Date of LO Acceptance",
            "Facility Agreement Date",
            "Expiry of Availability Period",
            "Maturity/Expired Date",
            "Tenure (Month)",
            "Operation Country",
            "Country Exposure",
            "State (if Country Exposure is Malaysia)",
            "Country Rating",
            "Region",
            "Market Type",
            "Classification of Entity / Customer Type",
            "Entity / Customer Type",
            "Classification of Residency Status",
            "Main Residency Status",
            "Residency Status",
            "Corporate Type",
            "Corporate Status",
            "Justification on Corporate Status ",
            "CCPT Classification",
            "Rating at Origination",
            "Internal Credit Rating (PD/PF)",
            "CRMS Obligor Risk Rating",
            "CRMS CG Rating",
            "PD (%)",
            "LGD (%)",
            "MFRS9 Staging",
            "BNM Main Sector",
            "BNM Sub Sector",
            "Oil and Gas (Y/N)",
            "Oil and Gas Segmentation\nDropdown - (Upstream, Midstream, Downstream)",
            "Annual Review Date",
            "Watchlist Review Date",
            "Payment/Repayment Frequency (Profit/Interest)",
            "Payment/Repayment Frequency (Cost/Principal)",
            "Effective cost of borrowings",
            "Profit/Interest Margin",
            "Effective Interest Rate (EIR)",
            "Ta`widh Compensation/Penalty Rate",
            "Date of Approval Restructured / Rescheduled",
            "Effective Date ( R&R )",
            "Reason Restructured & Resheduled",
            "Date Untagged from R&R",
            "Justification for Untagged",
            "Frequency of R&R ",
            "Grace Period (Month)",
            "Moratorium Period (Month) ",
            "Start Moratorium Date",
            "Date of Overdue",
            "Overdue (Days)",
            "Month in Arrears",
            "Overdue Amount (Facility Currency)",
            "Overdue Amount (MYR)",
            "Date Classified as Watchlist",
            "Watchlist Reason",
            "Date Declassified from Watchlist",
            "Date Impaired",
            "Reason for Impairment",
            "Partial Write off Date",
            "Write off Date",
            "Cancellation Date/Fully Settled Date",
            "Type of Collateral",
            "Collateral Amount ",
            "Relationship Manager (RM)",
            "Team",
            "Position as At"]]


COMBINE = MIS1.merge(LDB1.rename(columns={'facility_exim_account_num':'EXIM Account Number'}), on="EXIM Account Number", how='left',indicator=True)

COMBINE.loc[COMBINE["CIF Number"]==COMBINE["cif_number"],"dif_cif_number"] = "TRUE"
COMBINE.loc[COMBINE["CIF Number"]!=COMBINE["cif_number"],"dif_cif_number"] = "FALSE"

COMBINE.loc[COMBINE["Application System Code"]==COMBINE["facility_application_sys_code"],"dif_facility_application_sys_code"] = "TRUE"
COMBINE.loc[COMBINE["Application System Code"]!=COMBINE["facility_application_sys_code"],"dif_facility_application_sys_code"] = "FALSE"

COMBINE.loc[COMBINE["CCRIS Master Account Number"]==COMBINE["facility_ccris_master_account_num"],"dif_facility_ccris_master_account_num"] = "TRUE"
COMBINE.loc[COMBINE["CCRIS Master Account Number"]!=COMBINE["facility_ccris_master_account_num"],"dif_facility_ccris_master_account_num"] = "FALSE"

COMBINE.loc[COMBINE["CCRIS Sub Account Number"]==COMBINE["facility_ccris_sub_account_num"],"dif_facility_ccris_sub_account_num"] = "TRUE"
COMBINE.loc[COMBINE["CCRIS Sub Account Number"]!=COMBINE["facility_ccris_sub_account_num"],"dif_facility_ccris_sub_account_num"] = "FALSE"

COMBINE.loc[COMBINE["Finance SAP Number"]==COMBINE["finance_sap_number"],"dif_finance_sap_number"] = "TRUE"
COMBINE.loc[COMBINE["Finance SAP Number"]!=COMBINE["finance_sap_number"],"dif_finance_sap_number"] = "FALSE"

COMBINE.loc[COMBINE["Company Group"]==COMBINE["cif_company_group"],"dif_cif_company_group"] = "TRUE"
COMBINE.loc[COMBINE["Company Group"]!=COMBINE["cif_company_group"],"dif_cif_company_group"] = "FALSE"

COMBINE.loc[COMBINE["Customer Name"]==COMBINE["cif_name"],"dif_cif_name"] = "TRUE"
COMBINE.loc[COMBINE["Customer Name"]!=COMBINE["cif_name"],"dif_cif_name"] = "FALSE"

COMBINE.loc[COMBINE["Status of Account"]==COMBINE["acc_status"],"dif_acc_status"] = "TRUE"
COMBINE.loc[COMBINE["Status of Account"]!=COMBINE["acc_status"],"dif_acc_status"] = "FALSE"

COMBINE.loc[COMBINE["Application Type"]==COMBINE["dateapp_type"],"dif_dateapp_type"] = "TRUE"
COMBINE.loc[COMBINE["Application Type"]!=COMBINE["dateapp_type"],"dif_dateapp_type"] = "FALSE"

COMBINE.loc[COMBINE["Facility"]==COMBINE["facility_type_id"],"dif_facility_type_id"] = "TRUE"
COMBINE.loc[COMBINE["Facility"]!=COMBINE["facility_type_id"],"dif_facility_type_id"] = "FALSE"

#EXCEL TRUE
COMBINE.loc[COMBINE['Facility Currency']==COMBINE['facility_ccy_id'],'dif_facility_ccy_id'] = "TRUE"
# COMBINE.loc[COMBINE['Amount Approved / Facility Limit (Facility Currency)']==COMBINE['facility_amount_approved'],'dif_facility_amount_approved'] = "TRUE"
# COMBINE.loc[COMBINE['Amount Approved / Facility Limit (MYR)']==COMBINE['facility_amount_approved_myr'],'dif_facility_amount_approved_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cost/Principal Outstanding (Facility Currency)']==COMBINE['facility_amount_outstanding'],'dif_facility_amount_outstanding'] = "TRUE"
# COMBINE.loc[COMBINE['Cost/Principal Outstanding (MYR)']==COMBINE['acc_principal_amount_outstanding'],'dif_acc_principal_amount_outstanding'] = "TRUE"
# COMBINE.loc[COMBINE['Contingent Liability Letter of Credit (Facility Currency)']==COMBINE['acc_contingent_liability_letter_credit_fc'],'dif_acc_contingent_liability_letter_credit_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Contingent Liability Letter of Credit (MYR)']==COMBINE['acc_contingent_liability_letter_credit_myr'],'dif_acc_contingent_liability_letter_credit_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Contingent Liability (Facility Currency)']==COMBINE['acc_contingent_liability_ori'],'dif_acc_contingent_liability_ori'] = "TRUE"
# COMBINE.loc[COMBINE['Contingent Liability (MYR)']==COMBINE['acc_contingent_liability_myr'],'dif_acc_contingent_liability_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Account Receivables/Past Due Claims (Facility Currency)']==COMBINE['acc_receivables_past_due_claim_fc'],'dif_acc_receivables_past_due_claim_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Account Receivable/Past Due Claims (MYR)']==COMBINE['acc_receivable_past_due_claim_myr'],'dif_acc_receivable_past_due_claim_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Accrued Profit/Interest of the month(Facility Currency)']==COMBINE['acc_accrued_interest_month_fc'],'dif_acc_accrued_interest_month_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Accrued Profit/Interest of the month(MYR)']==COMBINE['acc_accrued_interest_month_myr'],'dif_acc_accrued_interest_month_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Modification of Loss (Facility Currency)']==COMBINE['modification_of_loss_fc'],'dif_modification_of_loss_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Modification of Loss (MYR)']==COMBINE['modification_of_loss_myr'],'dif_modification_of_loss_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Accrued Profit/Interest (Facility Currency)']==COMBINE['acc_accurate_interest'],'dif_acc_accurate_interest'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Accrued Profit/Interest (MYR)']==COMBINE['acc_accrued_interest_myr'],'dif_acc_accrued_interest_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Penalty/Ta`widh (Facility Currency)']==COMBINE['acc_penalty'],'dif_acc_penalty'] = "TRUE"
# COMBINE.loc[COMBINE['Penalty/Ta`widh (MYR)']==COMBINE['acc_penalty_myr'],'dif_acc_penalty_myr'] = "TRUE"
# COMBINE.loc[COMBINE["Ta'widh (Compensation) (Facility Currency)"]==COMBINE['acc_penalty_compensation_fc'],'dif_acc_penalty_compensation_fc'] = "TRUE"
# COMBINE.loc[COMBINE["Ta'widh (Compensation) (MYR)"]==COMBINE['acc_penalty_compensation_myr'],'dif_acc_penalty_compensation_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Income/Interest in Suspense (Facility Currency)']==COMBINE['acc_suspended_interest'],'dif_acc_suspended_interest'] = "TRUE"
# COMBINE.loc[COMBINE['Income/Interest in Suspense (MYR)']==COMBINE['acc_interest_suspense_myr'],'dif_acc_interest_suspense_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Other Charges (Facility Currency)']==COMBINE['acc_other_charges'],'dif_acc_other_charges'] = "TRUE"
# COMBINE.loc[COMBINE['Other Charges (MYR)']==COMBINE['acc_other_charges_myr'],'dif_acc_other_charges_myr'] = "TRUE"


# COMBINE.loc[COMBINE['Total Loans Outstanding FS (Facility Currency)']==COMBINE['acc_balance_outstanding_audited_fc'],'dif_acc_balance_outstanding_audited_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Total Loans Outstanding FS (MYR)']==COMBINE['acc_balance_outstanding_audited_myr'],'dif_acc_balance_outstanding_audited_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Total Banking Exposure (Facility Currency)']==COMBINE['acc_total_banking_exposure_fc'],'dif_acc_total_banking_exposure_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Total Banking Exposure (MYR)']==COMBINE['acc_total_banking_exposure_myr'],'dif_acc_total_banking_exposure_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Disbursement/Drawdown Status']==COMBINE['acc_disbursement_status'],'dif_acc_disbursement_status'] = "TRUE"
# COMBINE.loc[COMBINE['Expected Credit Loss (ECL) LAF (Facility Currency)']==COMBINE['acc_credit_loss_laf_ecl'],'dif_acc_credit_loss_laf_ecl'] = "TRUE"
# COMBINE.loc[COMBINE['Expected Credit Loss LAF (ECL) (MYR)']==COMBINE['acc_credit_loss_laf_ecl_myr'],'dif_acc_credit_loss_laf_ecl_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Expected Credit Loss C&C (ECL) (Facility Currency)']==COMBINE['acc_credit_loss_cnc_ecl'],'dif_acc_credit_loss_cnc_ecl'] = "TRUE"
# COMBINE.loc[COMBINE['Expected Credit Loss C&C (ECL) (MYR)']==COMBINE['acc_credit_loss_cnc_ecl_myr'],'dif_acc_credit_loss_cnc_ecl_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Unutilised/Undrawn Amount (Facility Currency)']==COMBINE['acc_undrawn_amount_banking_ori'],'dif_acc_undrawn_amount_banking_ori'] = "TRUE"
# COMBINE.loc[COMBINE['Unutilised/Undrawn Amount (MYR)']==COMBINE['acc_undrawn_amount_myr'],'dif_acc_undrawn_amount_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Disbursement/Drawdown (Facility Currency)']==COMBINE['acc_drawdown_fc'],'dif_acc_drawdown_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Disbursement/Drawdown (MYR)']==COMBINE['acc_drawdown_myr'],'dif_acc_drawdown_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Disbursement/Drawdown (Facility Currency)']==COMBINE['acc_cumulative_drawdown'],'dif_acc_cumulative_drawdown'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Disbursement/Drawdown (MYR)']==COMBINE['acc_cumulative_drawdown_myr'],'dif_acc_cumulative_drawdown_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cost Payment/Principal Repayment (Facility Currency)']==COMBINE['acc_repayment_fc'],'dif_acc_repayment_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Cost Payment/Principal Repayment (MYR)']==COMBINE['acc_repayment_myr'],'dif_acc_repayment_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Cost Payment/Principal Repayment (Facility ']==COMBINE['acc_cumulative_repayment'],'dif_acc_cumulative_repayment'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Cost Payment/Principal Repayment (MYR)']==COMBINE['acc_cumulative_repayment_myr'],'dif_acc_cumulative_repayment_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Profit Payment/Interest Repayment (Facility Currency)']==COMBINE['acc_interest_repayment_fc'],'dif_acc_interest_repayment_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Profit Payment/Interest Repayment (MYR)']==COMBINE['acc_interest_repayment_myr'],'dif_acc_interest_repayment_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Profit Payment/Interest Repayment (Facility ']==COMBINE['acc_cumulative_interest_repayment_fc'],'dif_acc_cumulative_interest_repayment_fc'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Profit Payment/Interest Repayment (MYR)']==COMBINE['acc_cumulative_interest_repayment_myr'],'dif_acc_cumulative_interest_repayment_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Ta`widh Payment/Penalty Repayment (Facility Currency)']==COMBINE['penalty_repayment'],'dif_penalty_repayment'] = "TRUE"
# COMBINE.loc[COMBINE['Ta`widh Payment/Penalty Repayment (MYR)']==COMBINE['penalty_repayment_myr'],'dif_penalty_repayment_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Ta`widh Payment/Penalty Repayment (Facility ']==COMBINE['cumulative_penalty'],'dif_cumulative_penalty'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Ta`widh Payment/Penalty Repayment (MYR)']==COMBINE['cumulative_penalty_myr'],'dif_cumulative_penalty_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Other Charges Payment (Facility Currency)']==COMBINE['other_charges_payment'],'dif_other_charges_payment'] = "TRUE"
# COMBINE.loc[COMBINE['Other Charges Payment (MYR)']==COMBINE['other_charges_payment_myr'],'dif_other_charges_payment_myr'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Other Charges Payment (Facility Currency)']==COMBINE['cumulative_other_charges_payment'],'dif_cumulative_other_charges_payment'] = "TRUE"
# COMBINE.loc[COMBINE['Cumulative Other Charges Payment (MYR)']==COMBINE['cumulative_other_charges_payment_myr'],'dif_cumulative_other_charges_payment_myr'] = "TRUE"
COMBINE.loc[COMBINE['Post Approval Stage']==COMBINE['ca_post_approval_stage'],'dif_ca_post_approval_stage'] = "TRUE"
COMBINE.loc[COMBINE['Date of Ready for Utilization (RU)']==COMBINE['date_ready_utilization'],'dif_date_ready_utilization'] = "TRUE"
COMBINE.loc[COMBINE['1st Disbursement Date / 1st Drawdown Date']==COMBINE['acc_first_disbursement_date'],'dif_acc_first_disbursement_date'] = "TRUE"
COMBINE.loc[COMBINE['1st Payment/Repayment Date']==COMBINE['acc_first_repayment_date'],'dif_acc_first_repayment_date'] = "TRUE"
COMBINE.loc[COMBINE['Nature of Account']==COMBINE['acc_nature_acc'],'dif_acc_nature_acc'] = "TRUE"
COMBINE.loc[COMBINE['Type of Financing']==COMBINE['financing_type'],'dif_financing_type'] = "TRUE"
COMBINE.loc[COMBINE['Shariah Contract / Concept']==COMBINE['shariah_concept'],'dif_shariah_concept'] = "TRUE"
COMBINE.loc[COMBINE['Syndicated / Club Deal']==COMBINE['syndicated_deal'],'dif_syndicated_deal'] = "TRUE"
COMBINE.loc[COMBINE['Fund Type']==COMBINE['fund_type'],'dif_fund_type'] = "TRUE"
COMBINE.loc[COMBINE['Incentive']==COMBINE['incentive'],'dif_incentive'] = "TRUE"
COMBINE.loc[COMBINE['Programme']==COMBINE['program_lending'],'dif_program_lending'] = "TRUE"
COMBINE.loc[COMBINE['Guarantee']==COMBINE['guarantee'],'dif_guarantee'] = "TRUE"
COMBINE.loc[COMBINE['Purpose of Financing']==COMBINE['purpose_financing'],'dif_purpose_financing'] = "TRUE"

COMBINE.loc[COMBINE['Date Approved at Origination']==COMBINE['approved_date'],'dif_approved_date'] = "TRUE"
COMBINE.loc[COMBINE['Approval Authority']==COMBINE['approval_authority'],'dif_approval_authority'] = "TRUE"
COMBINE.loc[COMBINE['LO issuance Date']==COMBINE['acc_lo_issuance_date'],'dif_acc_lo_issuance_date'] = "TRUE"
COMBINE.loc[COMBINE['Date of LO Acceptance']==COMBINE['acc_date_lo_acceptance'],'dif_acc_date_lo_acceptance'] = "TRUE"
COMBINE.loc[COMBINE['Facility Agreement Date']==COMBINE['acc_facility_agreement_date'],'dif_acc_facility_agreement_date'] = "TRUE"
COMBINE.loc[COMBINE['Expiry of Availability Period']==COMBINE['acc_availability_period'],'dif_acc_availability_period'] = "TRUE"
COMBINE.loc[COMBINE['Maturity/Expired Date']==COMBINE['acc_maturity_expired_date'],'dif_acc_maturity_expired_date'] = "TRUE"
COMBINE.loc[COMBINE['Tenure (Month)']==COMBINE['acc_tenure'],'dif_acc_tenure'] = "TRUE"
COMBINE.loc[COMBINE['Operation Country']==COMBINE['cif_operation_country'],'dif_cif_operation_country'] = "TRUE"
COMBINE.loc[COMBINE['Country Exposure']==COMBINE['facility_country_id'],'dif_facility_country_id'] = "TRUE"

COMBINE.loc[COMBINE['Country Rating']==COMBINE['acc_country_rating'],'dif_acc_country_rating'] = "TRUE"
COMBINE.loc[COMBINE['Region']==COMBINE['acc_region'],'dif_acc_region'] = "TRUE"
COMBINE.loc[COMBINE['Market Type']==COMBINE['market_type'],'dif_market_type'] = "TRUE"
COMBINE.loc[COMBINE['Classification of Entity / Customer Type']==COMBINE['classification_cust_type'],'dif_classification_cust_type'] = "TRUE"
COMBINE.loc[COMBINE['Entity / Customer Type']==COMBINE['cif_cust_type'],'dif_cif_cust_type'] = "TRUE"
COMBINE.loc[COMBINE['Classification of Residency Status']==COMBINE['classification_residency_status'],'dif_classification_residency_status'] = "TRUE"
COMBINE.loc[COMBINE['Main Residency Status']==COMBINE['classificationResidencyStatus'],'dif_classificationResidencyStatus'] = "TRUE"
COMBINE.loc[COMBINE['Residency Status']==COMBINE['cif_residency_status'],'dif_cif_residency_status'] = "TRUE"
COMBINE.loc[COMBINE['Corporate Type']==COMBINE['cif_corporate_type'],'dif_cif_corporate_type'] = "TRUE"
COMBINE.loc[COMBINE['Corporate Status']==COMBINE['cif_corporate_status'],'dif_cif_corporate_status'] = "TRUE"
COMBINE.loc[COMBINE['Justification on Corporate Status ']==COMBINE['justification_corporate_status'],'dif_justification_corporate_status'] = "TRUE"
COMBINE.loc[COMBINE['CCPT Classification']==COMBINE['ccpt_classification'],'dif_ccpt_classification'] = "TRUE"
COMBINE.loc[COMBINE['Rating at Origination']==COMBINE['acc_rating_origination'],'dif_acc_rating_origination'] = "TRUE"
COMBINE.loc[COMBINE['Internal Credit Rating (PD/PF)']==COMBINE['internal_credit_rating'],'dif_internal_credit_rating'] = "TRUE"
COMBINE.loc[COMBINE['CRMS Obligor Risk Rating']==COMBINE['crms_obligator_risk_rating'],'dif_crms_obligator_risk_rating'] = "TRUE"
COMBINE.loc[COMBINE['CRMS CG Rating']==COMBINE['crms_cg_rating'],'dif_crms_cg_rating'] = "TRUE"
COMBINE.loc[COMBINE['PD (%)']==COMBINE['pd_percent'],'dif_pd_percent'] = "TRUE"
COMBINE.loc[COMBINE['LGD (%)']==COMBINE['lgd_percent'],'dif_lgd_percent'] = "TRUE"
COMBINE.loc[COMBINE['MFRS9 Staging']==COMBINE['acc_MFRS9_staging'],'dif_acc_MFRS9_staging'] = "TRUE"
COMBINE.loc[COMBINE['BNM Main Sector']==COMBINE['bnm_main_sector'],'dif_bnm_main_sector'] = "TRUE"
COMBINE.loc[COMBINE['BNM Sub Sector']==COMBINE['bnm_sub_sector'],'dif_bnm_sub_sector'] = "TRUE"
COMBINE.loc[COMBINE['Oil and Gas (Y/N)']==COMBINE['oil_and_gas_desc'],'dif_oil_and_gas_desc'] = "TRUE"
COMBINE.loc[COMBINE['Oil and Gas Segmentation\nDropdown - (Upstream, Midstream, Downstream)']==COMBINE['oil_and_gas_segmentation'],'dif_oil_and_gas_segmentation'] = "TRUE"
COMBINE.loc[COMBINE['Annual Review Date']==COMBINE['acc_review_date'],'dif_acc_review_date'] = "TRUE"
COMBINE.loc[COMBINE['Watchlist Review Date']==COMBINE['acc_watchlist_review_date_approval'],'dif_acc_watchlist_review_date_approval'] = "TRUE"
COMBINE.loc[COMBINE['Payment/Repayment Frequency (Profit/Interest)']==COMBINE['acc_payment_frequency_interest'],'dif_acc_payment_frequency_interest'] = "TRUE"
COMBINE.loc[COMBINE['Payment/Repayment Frequency (Cost/Principal)']==COMBINE['acc_payment_frequency_principal'],'dif_acc_payment_frequency_principal'] = "TRUE"
COMBINE.loc[COMBINE['Effective cost of borrowings']==COMBINE['acc_effective_cost_borrowings'],'dif_acc_effective_cost_borrowings'] = "TRUE"
COMBINE.loc[COMBINE['Profit/Interest Margin']==COMBINE['acc_margin'],'dif_acc_margin'] = "TRUE"
COMBINE.loc[COMBINE['Effective Interest Rate (EIR)']==COMBINE['acc_average_interest_rate'],'dif_acc_average_interest_rate'] = "TRUE"
COMBINE.loc[COMBINE['Ta`widh Compensation/Penalty Rate']==COMBINE['acc_tadwih_compensation'],'dif_acc_tadwih_compensation'] = "TRUE"
COMBINE.loc[COMBINE['Date of Approval Restructured / Rescheduled']==COMBINE['dateapp_date'],'dif_dateapp_date'] = "TRUE"
COMBINE.loc[COMBINE['Effective Date ( R&R )']==COMBINE['dateapp_effectivedate'],'dif_dateapp_effectivedate'] = "TRUE"
COMBINE.loc[COMBINE['Reason Restructured & Resheduled']==COMBINE['dateapp_reason'],'dif_dateapp_reason'] = "TRUE"
COMBINE.loc[COMBINE['Date Untagged from R&R']==COMBINE['date_untagged_rr'],'dif_date_untagged_rr'] = "TRUE"
COMBINE.loc[COMBINE['Justification for Untagged']==COMBINE['justification_untagged'],'dif_justification_untagged'] = "TRUE"
COMBINE.loc[COMBINE['Frequency of R&R ']==COMBINE['frequency_rr'],'dif_frequency_rr'] = "TRUE"
COMBINE.loc[COMBINE['Grace Period (Month)']==COMBINE['acc_grace_period'],'dif_acc_grace_period'] = "TRUE"
COMBINE.loc[COMBINE['Moratorium Period (Month) ']==COMBINE['moratorium_period_month'],'dif_moratorium_period_month'] = "TRUE"
COMBINE.loc[COMBINE['Start Moratorium Date']==COMBINE['moratorium_start_date'],'dif_moratorium_start_date'] = "TRUE"
COMBINE.loc[COMBINE['Date of Overdue']==COMBINE['acc_date_overdue'],'dif_acc_date_overdue'] = "TRUE"
COMBINE.loc[COMBINE['Overdue (Days)']==COMBINE['acc_overdue_days'],'dif_acc_overdue_days'] = "TRUE"
COMBINE.loc[COMBINE['Month in Arrears']==COMBINE['int_month_in_arrears'],'dif_int_month_in_arrears'] = "TRUE"
COMBINE.loc[COMBINE['Overdue Amount (Facility Currency)']==COMBINE['acc_overdue_ori'],'dif_acc_overdue_ori'] = "TRUE"
COMBINE.loc[COMBINE['Overdue Amount (MYR)']==COMBINE['acc_overdue_amount_myr'],'dif_acc_overdue_amount_myr'] = "TRUE"
COMBINE.loc[COMBINE['Date Classified as Watchlist']==COMBINE['acc_watchlist_date'],'dif_acc_watchlist_date'] = "TRUE"
COMBINE.loc[COMBINE['Watchlist Reason']==COMBINE['acc_watchlist_reason'],'dif_acc_watchlist_reason'] = "TRUE"
COMBINE.loc[COMBINE['Date Declassified from Watchlist']==COMBINE['acc_date_delist_watchlist'],'dif_acc_date_delist_watchlist'] = "TRUE"
COMBINE.loc[COMBINE['Date Impaired']==COMBINE['acc_date_impaired'],'dif_acc_date_impaired'] = "TRUE"
COMBINE.loc[COMBINE['Reason for Impairment']==COMBINE['acc_reason_impairment'],'dif_acc_reason_impairment'] = "TRUE"
COMBINE.loc[COMBINE['Partial Write off Date']==COMBINE['acc_partial_writeoff_date'],'dif_acc_partial_writeoff_date'] = "TRUE"
COMBINE.loc[COMBINE['Write off Date']==COMBINE['acc_writeoff_date'],'dif_acc_writeoff_date'] = "TRUE"
COMBINE.loc[COMBINE['Cancellation Date/Fully Settled Date']==COMBINE['acc_cancel_fulltsettle_date'],'dif_acc_cancel_fulltsettle_date'] = "TRUE"


COMBINE.loc[COMBINE['Relationship Manager (RM)']==COMBINE['acc_relationship_manager_rm'],'dif_acc_relationship_manager_rm'] = "TRUE"
COMBINE.loc[COMBINE['Team']==COMBINE['acc_banking_team'],'dif_acc_banking_team'] = "TRUE"
COMBINE.loc[COMBINE['Position as At']==COMBINE['position_as_at'],'dif_position_as_at'] = "TRUE"


#EXCEL FALSE
COMBINE.loc[COMBINE['Facility Currency']!=COMBINE['facility_ccy_id'],'dif_facility_ccy_id'] = "FALSE"
# COMBINE.loc[COMBINE['Amount Approved / Facility Limit (Facility Currency)']!=COMBINE['facility_amount_approved'],'dif_facility_amount_approved'] = "FALSE"
# COMBINE.loc[COMBINE['Amount Approved / Facility Limit (MYR)']!=COMBINE['facility_amount_approved_myr'],'dif_facility_amount_approved_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cost/Principal Outstanding (Facility Currency)']!=COMBINE['facility_amount_outstanding'],'dif_facility_amount_outstanding'] = "FALSE"
# COMBINE.loc[COMBINE['Cost/Principal Outstanding (MYR)']!=COMBINE['acc_principal_amount_outstanding'],'dif_acc_principal_amount_outstanding'] = "FALSE"
# COMBINE.loc[COMBINE['Contingent Liability Letter of Credit (Facility Currency)']!=COMBINE['acc_contingent_liability_letter_credit_fc'],'dif_acc_contingent_liability_letter_credit_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Contingent Liability Letter of Credit (MYR)']!=COMBINE['acc_contingent_liability_letter_credit_myr'],'dif_acc_contingent_liability_letter_credit_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Contingent Liability (Facility Currency)']!=COMBINE['acc_contingent_liability_ori'],'dif_acc_contingent_liability_ori'] = "FALSE"
# COMBINE.loc[COMBINE['Contingent Liability (MYR)']!=COMBINE['acc_contingent_liability_myr'],'dif_acc_contingent_liability_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Account Receivables/Past Due Claims (Facility Currency)']!=COMBINE['acc_receivables_past_due_claim_fc'],'dif_acc_receivables_past_due_claim_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Account Receivable/Past Due Claims (MYR)']!=COMBINE['acc_receivable_past_due_claim_myr'],'dif_acc_receivable_past_due_claim_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Accrued Profit/Interest of the month(Facility Currency)']!=COMBINE['acc_accrued_interest_month_fc'],'dif_acc_accrued_interest_month_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Accrued Profit/Interest of the month(MYR)']!=COMBINE['acc_accrued_interest_month_myr'],'dif_acc_accrued_interest_month_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Modification of Loss (Facility Currency)']!=COMBINE['modification_of_loss_fc'],'dif_modification_of_loss_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Modification of Loss (MYR)']!=COMBINE['modification_of_loss_myr'],'dif_modification_of_loss_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Accrued Profit/Interest (Facility Currency)']!=COMBINE['acc_accurate_interest'],'dif_acc_accurate_interest'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Accrued Profit/Interest (MYR)']!=COMBINE['acc_accrued_interest_myr'],'dif_acc_accrued_interest_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Penalty/Ta`widh (Facility Currency)']!=COMBINE['acc_penalty'],'dif_acc_penalty'] = "FALSE"
# COMBINE.loc[COMBINE['Penalty/Ta`widh (MYR)']!=COMBINE['acc_penalty_myr'],'dif_acc_penalty_myr'] = "FALSE"
# COMBINE.loc[COMBINE["Ta'widh (Compensation) (Facility Currency)"]!=COMBINE['acc_penalty_compensation_fc'],'dif_acc_penalty_compensation_fc'] = "FALSE"
# COMBINE.loc[COMBINE["Ta'widh (Compensation) (MYR)"]!=COMBINE['acc_penalty_compensation_myr'],'dif_acc_penalty_compensation_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Income/Interest in Suspense (Facility Currency)']!=COMBINE['acc_suspended_interest'],'dif_acc_suspended_interest'] = "FALSE"
# COMBINE.loc[COMBINE['Income/Interest in Suspense (MYR)']!=COMBINE['acc_interest_suspense_myr'],'dif_acc_interest_suspense_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Other Charges (Facility Currency)']!=COMBINE['acc_other_charges'],'dif_acc_other_charges'] = "FALSE"
# COMBINE.loc[COMBINE['Other Charges (MYR)']!=COMBINE['acc_other_charges_myr'],'dif_acc_other_charges_myr'] = "FALSE"


# COMBINE.loc[COMBINE['Total Loans Outstanding FS (Facility Currency)']!=COMBINE['acc_balance_outstanding_audited_fc'],'dif_acc_balance_outstanding_audited_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Total Loans Outstanding FS (MYR)']!=COMBINE['acc_balance_outstanding_audited_myr'],'dif_acc_balance_outstanding_audited_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Total Banking Exposure (Facility Currency)']!=COMBINE['acc_total_banking_exposure_fc'],'dif_acc_total_banking_exposure_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Total Banking Exposure (MYR)']!=COMBINE['acc_total_banking_exposure_myr'],'dif_acc_total_banking_exposure_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Disbursement/Drawdown Status']!=COMBINE['acc_disbursement_status'],'dif_acc_disbursement_status'] = "FALSE"
# COMBINE.loc[COMBINE['Expected Credit Loss (ECL) LAF (Facility Currency)']!=COMBINE['acc_credit_loss_laf_ecl'],'dif_acc_credit_loss_laf_ecl'] = "FALSE"
# COMBINE.loc[COMBINE['Expected Credit Loss LAF (ECL) (MYR)']!=COMBINE['acc_credit_loss_laf_ecl_myr'],'dif_acc_credit_loss_laf_ecl_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Expected Credit Loss C&C (ECL) (Facility Currency)']!=COMBINE['acc_credit_loss_cnc_ecl'],'dif_acc_credit_loss_cnc_ecl'] = "FALSE"
# COMBINE.loc[COMBINE['Expected Credit Loss C&C (ECL) (MYR)']!=COMBINE['acc_credit_loss_cnc_ecl_myr'],'dif_acc_credit_loss_cnc_ecl_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Unutilised/Undrawn Amount (Facility Currency)']!=COMBINE['acc_undrawn_amount_banking_ori'],'dif_acc_undrawn_amount_banking_ori'] = "FALSE"
# COMBINE.loc[COMBINE['Unutilised/Undrawn Amount (MYR)']!=COMBINE['acc_undrawn_amount_myr'],'dif_acc_undrawn_amount_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Disbursement/Drawdown (Facility Currency)']!=COMBINE['acc_drawdown_fc'],'dif_acc_drawdown_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Disbursement/Drawdown (MYR)']!=COMBINE['acc_drawdown_myr'],'dif_acc_drawdown_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Disbursement/Drawdown (Facility Currency)']!=COMBINE['acc_cumulative_drawdown'],'dif_acc_cumulative_drawdown'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Disbursement/Drawdown (MYR)']!=COMBINE['acc_cumulative_drawdown_myr'],'dif_acc_cumulative_drawdown_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cost Payment/Principal Repayment (Facility Currency)']!=COMBINE['acc_repayment_fc'],'dif_acc_repayment_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Cost Payment/Principal Repayment (MYR)']!=COMBINE['acc_repayment_myr'],'dif_acc_repayment_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Cost Payment/Principal Repayment (Facility ']!=COMBINE['acc_cumulative_repayment'],'dif_acc_cumulative_repayment'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Cost Payment/Principal Repayment (MYR)']!=COMBINE['acc_cumulative_repayment_myr'],'dif_acc_cumulative_repayment_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Profit Payment/Interest Repayment (Facility Currency)']!=COMBINE['acc_interest_repayment_fc'],'dif_acc_interest_repayment_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Profit Payment/Interest Repayment (MYR)']!=COMBINE['acc_interest_repayment_myr'],'dif_acc_interest_repayment_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Profit Payment/Interest Repayment (Facility ']!=COMBINE['acc_cumulative_interest_repayment_fc'],'dif_acc_cumulative_interest_repayment_fc'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Profit Payment/Interest Repayment (MYR)']!=COMBINE['acc_cumulative_interest_repayment_myr'],'dif_acc_cumulative_interest_repayment_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Ta`widh Payment/Penalty Repayment (Facility Currency)']!=COMBINE['penalty_repayment'],'dif_penalty_repayment'] = "FALSE"
# COMBINE.loc[COMBINE['Ta`widh Payment/Penalty Repayment (MYR)']!=COMBINE['penalty_repayment_myr'],'dif_penalty_repayment_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Ta`widh Payment/Penalty Repayment (Facility ']!=COMBINE['cumulative_penalty'],'dif_cumulative_penalty'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Ta`widh Payment/Penalty Repayment (MYR)']!=COMBINE['cumulative_penalty_myr'],'dif_cumulative_penalty_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Other Charges Payment (Facility Currency)']!=COMBINE['other_charges_payment'],'dif_other_charges_payment'] = "FALSE"
# COMBINE.loc[COMBINE['Other Charges Payment (MYR)']!=COMBINE['other_charges_payment_myr'],'dif_other_charges_payment_myr'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Other Charges Payment (Facility Currency)']!=COMBINE['cumulative_other_charges_payment'],'dif_cumulative_other_charges_payment'] = "FALSE"
# COMBINE.loc[COMBINE['Cumulative Other Charges Payment (MYR)']!=COMBINE['cumulative_other_charges_payment_myr'],'dif_cumulative_other_charges_payment_myr'] = "FALSE"
COMBINE.loc[COMBINE['Post Approval Stage']!=COMBINE['ca_post_approval_stage'],'dif_ca_post_approval_stage'] = "FALSE"
COMBINE.loc[COMBINE['Date of Ready for Utilization (RU)']!=COMBINE['date_ready_utilization'],'dif_date_ready_utilization'] = "FALSE"
COMBINE.loc[COMBINE['1st Disbursement Date / 1st Drawdown Date']!=COMBINE['acc_first_disbursement_date'],'dif_acc_first_disbursement_date'] = "FALSE"
COMBINE.loc[COMBINE['1st Payment/Repayment Date']!=COMBINE['acc_first_repayment_date'],'dif_acc_first_repayment_date'] = "FALSE"
COMBINE.loc[COMBINE['Nature of Account']!=COMBINE['acc_nature_acc'],'dif_acc_nature_acc'] = "FALSE"
COMBINE.loc[COMBINE['Type of Financing']!=COMBINE['financing_type'],'dif_financing_type'] = "FALSE"
COMBINE.loc[COMBINE['Shariah Contract / Concept']!=COMBINE['shariah_concept'],'dif_shariah_concept'] = "FALSE"
COMBINE.loc[COMBINE['Syndicated / Club Deal']!=COMBINE['syndicated_deal'],'dif_syndicated_deal'] = "FALSE"
COMBINE.loc[COMBINE['Fund Type']!=COMBINE['fund_type'],'dif_fund_type'] = "FALSE"
COMBINE.loc[COMBINE['Incentive']!=COMBINE['incentive'],'dif_incentive'] = "FALSE"
COMBINE.loc[COMBINE['Programme']!=COMBINE['program_lending'],'dif_program_lending'] = "FALSE"
COMBINE.loc[COMBINE['Guarantee']!=COMBINE['guarantee'],'dif_guarantee'] = "FALSE"
COMBINE.loc[COMBINE['Purpose of Financing']!=COMBINE['purpose_financing'],'dif_purpose_financing'] = "FALSE"

COMBINE.loc[COMBINE['Date Approved at Origination']!=COMBINE['approved_date'],'dif_approved_date'] = "FALSE"
COMBINE.loc[COMBINE['Approval Authority']!=COMBINE['approval_authority'],'dif_approval_authority'] = "FALSE"
COMBINE.loc[COMBINE['LO issuance Date']!=COMBINE['acc_lo_issuance_date'],'dif_acc_lo_issuance_date'] = "FALSE"
COMBINE.loc[COMBINE['Date of LO Acceptance']!=COMBINE['acc_date_lo_acceptance'],'dif_acc_date_lo_acceptance'] = "FALSE"
COMBINE.loc[COMBINE['Facility Agreement Date']!=COMBINE['acc_facility_agreement_date'],'dif_acc_facility_agreement_date'] = "FALSE"
COMBINE.loc[COMBINE['Expiry of Availability Period']!=COMBINE['acc_availability_period'],'dif_acc_availability_period'] = "FALSE"
COMBINE.loc[COMBINE['Maturity/Expired Date']!=COMBINE['acc_maturity_expired_date'],'dif_acc_maturity_expired_date'] = "FALSE"
COMBINE.loc[COMBINE['Tenure (Month)']!=COMBINE['acc_tenure'],'dif_acc_tenure'] = "FALSE"
COMBINE.loc[COMBINE['Operation Country']!=COMBINE['cif_operation_country'],'dif_cif_operation_country'] = "FALSE"
COMBINE.loc[COMBINE['Country Exposure']!=COMBINE['facility_country_id'],'dif_facility_country_id'] = "FALSE"

COMBINE.loc[COMBINE['Country Rating']!=COMBINE['acc_country_rating'],'dif_acc_country_rating'] = "FALSE"
COMBINE.loc[COMBINE['Region']!=COMBINE['acc_region'],'dif_acc_region'] = "FALSE"
COMBINE.loc[COMBINE['Market Type']!=COMBINE['market_type'],'dif_market_type'] = "FALSE"
COMBINE.loc[COMBINE['Classification of Entity / Customer Type']!=COMBINE['classification_cust_type'],'dif_classification_cust_type'] = "FALSE"
COMBINE.loc[COMBINE['Entity / Customer Type']!=COMBINE['cif_cust_type'],'dif_cif_cust_type'] = "FALSE"
COMBINE.loc[COMBINE['Classification of Residency Status']!=COMBINE['classification_residency_status'],'dif_classification_residency_status'] = "FALSE"
COMBINE.loc[COMBINE['Main Residency Status']!=COMBINE['classificationResidencyStatus'],'dif_classificationResidencyStatus'] = "FALSE"
COMBINE.loc[COMBINE['Residency Status']!=COMBINE['cif_residency_status'],'dif_cif_residency_status'] = "FALSE"
COMBINE.loc[COMBINE['Corporate Type']!=COMBINE['cif_corporate_type'],'dif_cif_corporate_type'] = "FALSE"
COMBINE.loc[COMBINE['Corporate Status']!=COMBINE['cif_corporate_status'],'dif_cif_corporate_status'] = "FALSE"
COMBINE.loc[COMBINE['Justification on Corporate Status ']!=COMBINE['justification_corporate_status'],'dif_justification_corporate_status'] = "FALSE"
COMBINE.loc[COMBINE['CCPT Classification']!=COMBINE['ccpt_classification'],'dif_ccpt_classification'] = "FALSE"
COMBINE.loc[COMBINE['Rating at Origination']!=COMBINE['acc_rating_origination'],'dif_acc_rating_origination'] = "FALSE"
COMBINE.loc[COMBINE['Internal Credit Rating (PD/PF)']!=COMBINE['internal_credit_rating'],'dif_internal_credit_rating'] = "FALSE"
COMBINE.loc[COMBINE['CRMS Obligor Risk Rating']!=COMBINE['crms_obligator_risk_rating'],'dif_crms_obligator_risk_rating'] = "FALSE"
COMBINE.loc[COMBINE['CRMS CG Rating']!=COMBINE['crms_cg_rating'],'dif_crms_cg_rating'] = "FALSE"
COMBINE.loc[COMBINE['PD (%)']!=COMBINE['pd_percent'],'dif_pd_percent'] = "FALSE"
COMBINE.loc[COMBINE['LGD (%)']!=COMBINE['lgd_percent'],'dif_lgd_percent'] = "FALSE"
COMBINE.loc[COMBINE['MFRS9 Staging']!=COMBINE['acc_MFRS9_staging'],'dif_acc_MFRS9_staging'] = "FALSE"
COMBINE.loc[COMBINE['BNM Main Sector']!=COMBINE['bnm_main_sector'],'dif_bnm_main_sector'] = "FALSE"
COMBINE.loc[COMBINE['BNM Sub Sector']!=COMBINE['bnm_sub_sector'],'dif_bnm_sub_sector'] = "FALSE"
COMBINE.loc[COMBINE['Oil and Gas (Y/N)']!=COMBINE['oil_and_gas_desc'],'dif_oil_and_gas_desc'] = "FALSE"
COMBINE.loc[COMBINE['Oil and Gas Segmentation\nDropdown - (Upstream, Midstream, Downstream)']!=COMBINE['oil_and_gas_segmentation'],'dif_oil_and_gas_segmentation'] = "FALSE"
COMBINE.loc[COMBINE['Annual Review Date']!=COMBINE['acc_review_date'],'dif_acc_review_date'] = "FALSE"
COMBINE.loc[COMBINE['Watchlist Review Date']!=COMBINE['acc_watchlist_review_date_approval'],'dif_acc_watchlist_review_date_approval'] = "FALSE"
COMBINE.loc[COMBINE['Payment/Repayment Frequency (Profit/Interest)']!=COMBINE['acc_payment_frequency_interest'],'dif_acc_payment_frequency_interest'] = "FALSE"
COMBINE.loc[COMBINE['Payment/Repayment Frequency (Cost/Principal)']!=COMBINE['acc_payment_frequency_principal'],'dif_acc_payment_frequency_principal'] = "FALSE"
COMBINE.loc[COMBINE['Effective cost of borrowings']!=COMBINE['acc_effective_cost_borrowings'],'dif_acc_effective_cost_borrowings'] = "FALSE"
COMBINE.loc[COMBINE['Profit/Interest Margin']!=COMBINE['acc_margin'],'dif_acc_margin'] = "FALSE"
COMBINE.loc[COMBINE['Effective Interest Rate (EIR)']!=COMBINE['acc_average_interest_rate'],'dif_acc_average_interest_rate'] = "FALSE"
COMBINE.loc[COMBINE['Ta`widh Compensation/Penalty Rate']!=COMBINE['acc_tadwih_compensation'],'dif_acc_tadwih_compensation'] = "FALSE"
COMBINE.loc[COMBINE['Date of Approval Restructured / Rescheduled']!=COMBINE['dateapp_date'],'dif_dateapp_date'] = "FALSE"
COMBINE.loc[COMBINE['Effective Date ( R&R )']!=COMBINE['dateapp_effectivedate'],'dif_dateapp_effectivedate'] = "FALSE"
COMBINE.loc[COMBINE['Reason Restructured & Resheduled']!=COMBINE['dateapp_reason'],'dif_dateapp_reason'] = "FALSE"
COMBINE.loc[COMBINE['Date Untagged from R&R']!=COMBINE['date_untagged_rr'],'dif_date_untagged_rr'] = "FALSE"
COMBINE.loc[COMBINE['Justification for Untagged']!=COMBINE['justification_untagged'],'dif_justification_untagged'] = "FALSE"
COMBINE.loc[COMBINE['Frequency of R&R ']!=COMBINE['frequency_rr'],'dif_frequency_rr'] = "FALSE"
COMBINE.loc[COMBINE['Grace Period (Month)']!=COMBINE['acc_grace_period'],'dif_acc_grace_period'] = "FALSE"
COMBINE.loc[COMBINE['Moratorium Period (Month) ']!=COMBINE['moratorium_period_month'],'dif_moratorium_period_month'] = "FALSE"
COMBINE.loc[COMBINE['Start Moratorium Date']!=COMBINE['moratorium_start_date'],'dif_moratorium_start_date'] = "FALSE"
COMBINE.loc[COMBINE['Date of Overdue']!=COMBINE['acc_date_overdue'],'dif_acc_date_overdue'] = "FALSE"
COMBINE.loc[COMBINE['Overdue (Days)']!=COMBINE['acc_overdue_days'],'dif_acc_overdue_days'] = "FALSE"
COMBINE.loc[COMBINE['Month in Arrears']!=COMBINE['int_month_in_arrears'],'dif_int_month_in_arrears'] = "FALSE"
COMBINE.loc[COMBINE['Overdue Amount (Facility Currency)']!=COMBINE['acc_overdue_ori'],'dif_acc_overdue_ori'] = "FALSE"
COMBINE.loc[COMBINE['Overdue Amount (MYR)']!=COMBINE['acc_overdue_amount_myr'],'dif_acc_overdue_amount_myr'] = "FALSE"
COMBINE.loc[COMBINE['Date Classified as Watchlist']!=COMBINE['acc_watchlist_date'],'dif_acc_watchlist_date'] = "FALSE"
COMBINE.loc[COMBINE['Watchlist Reason']!=COMBINE['acc_watchlist_reason'],'dif_acc_watchlist_reason'] = "FALSE"
COMBINE.loc[COMBINE['Date Declassified from Watchlist']!=COMBINE['acc_date_delist_watchlist'],'dif_acc_date_delist_watchlist'] = "FALSE"
COMBINE.loc[COMBINE['Date Impaired']!=COMBINE['acc_date_impaired'],'dif_acc_date_impaired'] = "FALSE"
COMBINE.loc[COMBINE['Reason for Impairment']!=COMBINE['acc_reason_impairment'],'dif_acc_reason_impairment'] = "FALSE"
COMBINE.loc[COMBINE['Partial Write off Date']!=COMBINE['acc_partial_writeoff_date'],'dif_acc_partial_writeoff_date'] = "FALSE"
COMBINE.loc[COMBINE['Write off Date']!=COMBINE['acc_writeoff_date'],'dif_acc_writeoff_date'] = "FALSE"
COMBINE.loc[COMBINE['Cancellation Date/Fully Settled Date']!=COMBINE['acc_cancel_fulltsettle_date'],'dif_acc_cancel_fulltsettle_date'] = "FALSE"


COMBINE.loc[COMBINE['Relationship Manager (RM)']!=COMBINE['acc_relationship_manager_rm'],'dif_acc_relationship_manager_rm'] = "FALSE"
COMBINE.loc[COMBINE['Team']!=COMBINE['acc_banking_team'],'dif_acc_banking_team'] = "FALSE"
COMBINE.loc[COMBINE['Position as At']!=COMBINE['position_as_at'],'dif_position_as_at'] = "FALSE"

#Formula
COMBINE['Amount Approved / Facility Limit (Facility Currency)']=COMBINE['Amount Approved / Facility Limit (Facility Currency)'].str.replace(",", "")
COMBINE['dif_facility_amount_approved']=COMBINE['Amount Approved / Facility Limit (Facility Currency)'].astype(float)-COMBINE['facility_amount_approved'].astype(float)

COMBINE['Amount Approved / Facility Limit (MYR)']=COMBINE['Amount Approved / Facility Limit (MYR)'].str.replace(",", "")
COMBINE['dif_facility_amount_approved_myr']=COMBINE['Amount Approved / Facility Limit (MYR)'].astype(float)-COMBINE['facility_amount_approved_myr'].astype(float)

COMBINE['Cost/Principal Outstanding (Facility Currency)']=COMBINE['Cost/Principal Outstanding (Facility Currency)'].str.replace(",", "")
COMBINE['dif_facility_amount_outstanding']=COMBINE['Cost/Principal Outstanding (Facility Currency)'].astype(float)-COMBINE['facility_amount_outstanding'].astype(float)

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_principal_amount_outstanding']=COMBINE['Cost/Principal Outstanding (MYR)'].astype(float)-COMBINE['acc_principal_amount_outstanding'].astype(float)

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_contingent_liability_letter_credit_fc']=COMBINE['Contingent Liability Letter of Credit (Facility Currency)'].astype(float)-COMBINE['acc_contingent_liability_letter_credit_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_contingent_liability_letter_credit_myr']=COMBINE['Contingent Liability Letter of Credit (MYR)'].astype(float)-COMBINE['acc_contingent_liability_letter_credit_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_contingent_liability_ori']=COMBINE['Contingent Liability (Facility Currency)'].astype(float)-COMBINE['acc_contingent_liability_ori']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_contingent_liability_myr']=COMBINE['Contingent Liability (MYR)'].astype(float)-COMBINE['acc_contingent_liability_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_receivables_past_due_claim_fc']=COMBINE['Account Receivables/Past Due Claims (Facility Currency)'].astype(float)-COMBINE['acc_receivables_past_due_claim_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_receivable_past_due_claim_myr']=COMBINE['Account Receivable/Past Due Claims (MYR)'].astype(float)-COMBINE['acc_receivable_past_due_claim_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_accrued_interest_month_fc']=COMBINE['Accrued Profit/Interest of the month(Facility Currency)'].astype(float)-COMBINE['acc_accrued_interest_month_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_accrued_interest_month_myr']=COMBINE['Accrued Profit/Interest of the month(MYR)'].astype(float)-COMBINE['acc_accrued_interest_month_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_modification_of_loss_fc']=COMBINE['Modification of Loss (Facility Currency)'].astype(float)-COMBINE['modification_of_loss_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_modification_of_loss_myr']=COMBINE['Modification of Loss (MYR)'].astype(float)-COMBINE['modification_of_loss_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_accurate_interest']=COMBINE['Cumulative Accrued Profit/Interest (Facility Currency)'].astype(float)-COMBINE['acc_accurate_interest']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_accrued_interest_myr']=COMBINE['Cumulative Accrued Profit/Interest (MYR)'].astype(float)-COMBINE['acc_accrued_interest_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_penalty']=COMBINE['Penalty/Ta`widh (Facility Currency)'].astype(float)-COMBINE['acc_penalty']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_penalty_myr']=COMBINE['Penalty/Ta`widh (MYR)'].astype(float)-COMBINE['acc_penalty_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_penalty_compensation_fc']=COMBINE["Ta'widh (Compensation) (Facility Currency)"].astype(float)-COMBINE['acc_penalty_compensation_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_penalty_compensation_myr']=COMBINE["Ta'widh (Compensation) (MYR)"].astype(float)-COMBINE['acc_penalty_compensation_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_suspended_interest']=COMBINE['Income/Interest in Suspense (Facility Currency)'].astype(float)-COMBINE['acc_suspended_interest']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_interest_suspense_myr']=COMBINE['Income/Interest in Suspense (MYR)'].astype(float)-COMBINE['acc_interest_suspense_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_other_charges']=COMBINE['Other Charges (Facility Currency)'].astype(float)-COMBINE['acc_other_charges']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_other_charges_myr']=COMBINE['Other Charges (MYR)'].astype(float)-COMBINE['acc_other_charges_myr']


COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_balance_outstanding_audited_fc']=COMBINE['Total Loans Outstanding FS (Facility Currency)'].astype(float)-COMBINE['acc_balance_outstanding_audited_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_balance_outstanding_audited_myr']=COMBINE['Total Loans Outstanding FS (MYR)'].astype(float)-COMBINE['acc_balance_outstanding_audited_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_total_banking_exposure_fc']=COMBINE['Total Banking Exposure (Facility Currency)'].astype(float)-COMBINE['acc_total_banking_exposure_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_total_banking_exposure_myr']=COMBINE['Total Banking Exposure (MYR)'].astype(float)-COMBINE['acc_total_banking_exposure_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_disbursement_status']=COMBINE['Disbursement/Drawdown Status'].astype(float)-COMBINE['acc_disbursement_status']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_credit_loss_laf_ecl']=COMBINE['Expected Credit Loss (ECL) LAF (Facility Currency)'].astype(float)-COMBINE['acc_credit_loss_laf_ecl']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_credit_loss_laf_ecl_myr']=COMBINE['Expected Credit Loss LAF (ECL) (MYR)'].astype(float)-COMBINE['acc_credit_loss_laf_ecl_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_credit_loss_cnc_ecl']=COMBINE['Expected Credit Loss C&C (ECL) (Facility Currency)'].astype(float)-COMBINE['acc_credit_loss_cnc_ecl']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_credit_loss_cnc_ecl_myr']=COMBINE['Expected Credit Loss C&C (ECL) (MYR)'].astype(float)-COMBINE['acc_credit_loss_cnc_ecl_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_undrawn_amount_banking_ori']=COMBINE['Unutilised/Undrawn Amount (Facility Currency)'].astype(float)-COMBINE['acc_undrawn_amount_banking_ori']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_undrawn_amount_myr']=COMBINE['Unutilised/Undrawn Amount (MYR)'].astype(float)-COMBINE['acc_undrawn_amount_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_drawdown_fc']=COMBINE['Disbursement/Drawdown (Facility Currency)'].astype(float)-COMBINE['acc_drawdown_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_drawdown_myr']=COMBINE['Disbursement/Drawdown (MYR)'].astype(float)-COMBINE['acc_drawdown_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_cumulative_drawdown']=COMBINE['Cumulative Disbursement/Drawdown (Facility Currency)'].astype(float)-COMBINE['acc_cumulative_drawdown']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_cumulative_drawdown_myr']=COMBINE['Cumulative Disbursement/Drawdown (MYR)'].astype(float)-COMBINE['acc_cumulative_drawdown_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_repayment_fc']=COMBINE['Cost Payment/Principal Repayment (Facility Currency)'].astype(float)-COMBINE['acc_repayment_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_repayment_myr']=COMBINE['Cost Payment/Principal Repayment (MYR)'].astype(float)-COMBINE['acc_repayment_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_cumulative_repayment']=COMBINE['Cumulative Cost Payment/Principal Repayment (Facility '].astype(float)-COMBINE['acc_cumulative_repayment']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_cumulative_repayment_myr']=COMBINE['Cumulative Cost Payment/Principal Repayment (MYR)'].astype(float)-COMBINE['acc_cumulative_repayment_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_interest_repayment_fc']=COMBINE['Profit Payment/Interest Repayment (Facility Currency)'].astype(float)-COMBINE['acc_interest_repayment_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_interest_repayment_myr']=COMBINE['Profit Payment/Interest Repayment (MYR)'].astype(float)-COMBINE['acc_interest_repayment_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_cumulative_interest_repayment_fc']=COMBINE['Cumulative Profit Payment/Interest Repayment (Facility '].astype(float)-COMBINE['acc_cumulative_interest_repayment_fc']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_acc_cumulative_interest_repayment_myr']=COMBINE['Cumulative Profit Payment/Interest Repayment (MYR)'].astype(float)-COMBINE['acc_cumulative_interest_repayment_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_penalty_repayment']=COMBINE['Ta`widh Payment/Penalty Repayment (Facility Currency)'].astype(float)-COMBINE['penalty_repayment']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_penalty_repayment_myr']=COMBINE['Ta`widh Payment/Penalty Repayment (MYR)'].astype(float)-COMBINE['penalty_repayment_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_cumulative_penalty']=COMBINE['Cumulative Ta`widh Payment/Penalty Repayment (Facility '].astype(float)-COMBINE['cumulative_penalty']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_cumulative_penalty_myr']=COMBINE['Cumulative Ta`widh Payment/Penalty Repayment (MYR)'].astype(float)-COMBINE['cumulative_penalty_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_other_charges_payment']=COMBINE['Other Charges Payment (Facility Currency)'].astype(float)-COMBINE['other_charges_payment']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_other_charges_payment_myr']=COMBINE['Other Charges Payment (MYR)'].astype(float)-COMBINE['other_charges_payment_myr']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_cumulative_other_charges_payment']=COMBINE['Cumulative Other Charges Payment (Facility Currency)'].astype(float)-COMBINE['cumulative_other_charges_payment']

COMBINE['']=COMBINE[''].str.replace(",", "")
COMBINE['dif_cumulative_other_charges_payment_myr']=COMBINE['Cumulative Other Charges Payment (MYR)'].astype(float)-COMBINE['cumulative_other_charges_payment_myr']


COMBINE1= COMBINE[["EXIM Account Number",
"_merge",
"CIF Number",
"cif_number",
"dif_cif_number",
"Application System Code",
"facility_application_sys_code",
"dif_facility_application_sys_code",
"CCRIS Master Account Number",
"facility_ccris_master_account_num",
"dif_facility_ccris_master_account_num",
"CCRIS Sub Account Number",
"facility_ccris_sub_account_num",
"dif_facility_ccris_sub_account_num",
"Finance SAP Number",
"finance_sap_number",
"dif_finance_sap_number",
"Company Group",
"cif_company_group",
"dif_cif_company_group",
"Customer Name",
"cif_name",
"dif_cif_name",
"Status of Account",
"acc_status",
"dif_acc_status",
"Application Type",
"dateapp_type",
"dif_dateapp_type",
"Facility",
"facility_type_id",
"dif_facility_type_id",
"BNM Facility Type Classification (CCRIS)",
"Facility Currency",
"facility_ccy_id",
"dif_facility_ccy_id",
"Amount Approved / Facility Limit (Facility Currency)",
"facility_amount_approved",
"dif_facility_amount_approved",
"Amount Approved / Facility Limit (MYR)",
"facility_amount_approved_myr",
"dif_facility_amount_approved_myr",
"Financing Size",
"Cost/Principal Outstanding (Facility Currency)",
"facility_amount_outstanding",
"dif_facility_amount_outstanding",
"Cost/Principal Outstanding (MYR)",
"acc_principal_amount_outstanding",
"dif_acc_principal_amount_outstanding",
"Contingent Liability Letter of Credit (Facility Currency)",
"acc_contingent_liability_letter_credit_fc",
"dif_acc_contingent_liability_letter_credit_fc",
"Contingent Liability Letter of Credit (MYR)",
"acc_contingent_liability_letter_credit_myr",
"dif_acc_contingent_liability_letter_credit_myr",
"Contingent Liability (Facility Currency)",
"acc_contingent_liability_ori",
"dif_acc_contingent_liability_ori",
"Contingent Liability (MYR)",
"acc_contingent_liability_myr",
"dif_acc_contingent_liability_myr",
"Account Receivables/Past Due Claims (Facility Currency)",
"acc_receivables_past_due_claim_fc",
"dif_acc_receivables_past_due_claim_fc",
"Account Receivable/Past Due Claims (MYR)",
"acc_receivable_past_due_claim_myr",
"dif_acc_receivable_past_due_claim_myr",
"Accrued Profit/Interest of the month(Facility Currency)",
"acc_accrued_interest_month_fc",
"dif_acc_accrued_interest_month_fc",
"Accrued Profit/Interest of the month(MYR)",
"acc_accrued_interest_month_myr",
"dif_acc_accrued_interest_month_myr",
"Modification of Loss (Facility Currency)",
"modification_of_loss_fc",
"dif_modification_of_loss_fc",
"Modification of Loss (MYR)",
"modification_of_loss_myr",
"dif_modification_of_loss_myr",
"Cumulative Accrued Profit/Interest (Facility Currency)",
"acc_accurate_interest",
"dif_acc_accurate_interest",
"Cumulative Accrued Profit/Interest (MYR)",
"acc_accrued_interest_myr",
"dif_acc_accrued_interest_myr",
"Penalty/Ta`widh (Facility Currency)",
"acc_penalty",
"dif_acc_penalty",
"Penalty/Ta`widh (MYR)",
"acc_penalty_myr",
"dif_acc_penalty_myr",
"Ta'widh (Compensation) (Facility Currency)",
"acc_penalty_compensation_fc",
"dif_acc_penalty_compensation_fc",
"Ta'widh (Compensation) (MYR)",
"acc_penalty_compensation_myr",
"dif_acc_penalty_compensation_myr",
"Income/Interest in Suspense (Facility Currency)",
"acc_suspended_interest",
"dif_acc_suspended_interest",
"Income/Interest in Suspense (MYR)",
"acc_interest_suspense_myr",
"dif_acc_interest_suspense_myr",
"Other Charges (Facility Currency)",
"acc_other_charges",
"dif_acc_other_charges",
"Other Charges (MYR)",
"acc_other_charges_myr",
"dif_acc_other_charges_myr",
"Total Loans Outstanding CCRIS (Facility Currency)",
"Total Loans Outstanding CCRIS (MYR)",
"Total Loans Outstanding FS (Facility Currency)",
"acc_balance_outstanding_audited_fc",
"dif_acc_balance_outstanding_audited_fc",
"Total Loans Outstanding FS (MYR)",
"acc_balance_outstanding_audited_myr",
"dif_acc_balance_outstanding_audited_myr",
"Total Banking Exposure (Facility Currency)",
"acc_total_banking_exposure_fc",
"dif_acc_total_banking_exposure_fc",
"Total Banking Exposure (MYR)",
"acc_total_banking_exposure_myr",
"dif_acc_total_banking_exposure_myr",
"Disbursement/Drawdown Status",
"acc_disbursement_status",
"dif_acc_disbursement_status",
"Expected Credit Loss (ECL) LAF (Facility Currency)",
"acc_credit_loss_laf_ecl",
"dif_acc_credit_loss_laf_ecl",
"Expected Credit Loss LAF (ECL) (MYR)",
"acc_credit_loss_laf_ecl_myr",
"dif_acc_credit_loss_laf_ecl_myr",
"Expected Credit Loss C&C (ECL) (Facility Currency)",
"acc_credit_loss_cnc_ecl",
"dif_acc_credit_loss_cnc_ecl",
"Expected Credit Loss C&C (ECL) (MYR)",
"acc_credit_loss_cnc_ecl_myr",
"dif_acc_credit_loss_cnc_ecl_myr",
"Unutilised/Undrawn Amount (Facility Currency)",
"acc_undrawn_amount_banking_ori",
"dif_acc_undrawn_amount_banking_ori",
"Unutilised/Undrawn Amount (MYR)",
"acc_undrawn_amount_myr",
"dif_acc_undrawn_amount_myr",
"Disbursement/Drawdown (Facility Currency)",
"acc_drawdown_fc",
"dif_acc_drawdown_fc",
"Disbursement/Drawdown (MYR)",
"acc_drawdown_myr",
"dif_acc_drawdown_myr",
"Cumulative Disbursement/Drawdown (Facility Currency)",
"acc_cumulative_drawdown",
"dif_acc_cumulative_drawdown",
"Cumulative Disbursement/Drawdown (MYR)",
"acc_cumulative_drawdown_myr",
"dif_acc_cumulative_drawdown_myr",
"Cost Payment/Principal Repayment (Facility Currency)",
"acc_repayment_fc",
"dif_acc_repayment_fc",
"Cost Payment/Principal Repayment (MYR)",
"acc_repayment_myr",
"dif_acc_repayment_myr",
"Cumulative Cost Payment/Principal Repayment (Facility ",
"acc_cumulative_repayment",
"dif_acc_cumulative_repayment",
"Cumulative Cost Payment/Principal Repayment (MYR)",
"acc_cumulative_repayment_myr",
"dif_acc_cumulative_repayment_myr",
"Profit Payment/Interest Repayment (Facility Currency)",
"acc_interest_repayment_fc",
"dif_acc_interest_repayment_fc",
"Profit Payment/Interest Repayment (MYR)",
"acc_interest_repayment_myr",
"dif_acc_interest_repayment_myr",
"Cumulative Profit Payment/Interest Repayment (Facility ",
"acc_cumulative_interest_repayment_fc",
"dif_acc_cumulative_interest_repayment_fc",
"Cumulative Profit Payment/Interest Repayment (MYR)",
"acc_cumulative_interest_repayment_myr",
"dif_acc_cumulative_interest_repayment_myr",
"Ta`widh Payment/Penalty Repayment (Facility Currency)",
"penalty_repayment",
"dif_penalty_repayment",
"Ta`widh Payment/Penalty Repayment (MYR)",
"penalty_repayment_myr",
"dif_penalty_repayment_myr",
"Cumulative Ta`widh Payment/Penalty Repayment (Facility ",
"cumulative_penalty",
"dif_cumulative_penalty",
"Cumulative Ta`widh Payment/Penalty Repayment (MYR)",
"cumulative_penalty_myr",
"dif_cumulative_penalty_myr",
"Other Charges Payment (Facility Currency)",
"other_charges_payment",
"dif_other_charges_payment",
"Other Charges Payment (MYR)",
"other_charges_payment_myr",
"dif_other_charges_payment_myr",
"Cumulative Other Charges Payment (Facility Currency)",
"cumulative_other_charges_payment",
"dif_cumulative_other_charges_payment",
"Cumulative Other Charges Payment (MYR)",
"cumulative_other_charges_payment_myr",
"dif_cumulative_other_charges_payment_myr",
"Post Approval Stage",
"ca_post_approval_stage",
"dif_ca_post_approval_stage",
"Date of Ready for Utilization (RU)",
"date_ready_utilization",
"dif_date_ready_utilization",
"1st Disbursement Date / 1st Drawdown Date",
"acc_first_disbursement_date",
"dif_acc_first_disbursement_date",
"1st Payment/Repayment Date",
"acc_first_repayment_date",
"dif_acc_first_repayment_date",
"Nature of Account",
"acc_nature_acc",
"dif_acc_nature_acc",
"Type of Financing",
"financing_type",
"dif_financing_type",
"Shariah Contract / Concept",
"shariah_concept",
"dif_shariah_concept",
"Syndicated / Club Deal",
"syndicated_deal",
"dif_syndicated_deal",
"Fund Type",
"fund_type",
"dif_fund_type",
"Incentive",
"incentive",
"dif_incentive",
"Programme",
"program_lending",
"dif_program_lending",
"Guarantee",
"guarantee",
"dif_guarantee",
"Purpose of Financing",
"purpose_financing",
"dif_purpose_financing",
"BNM Purpose of Loan",
"Date Approved at Origination",
"approved_date",
"dif_approved_date",
"Approval Authority",
"approval_authority",
"dif_approval_authority",
"LO issuance Date",
"acc_lo_issuance_date",
"dif_acc_lo_issuance_date",
"Date of LO Acceptance",
"acc_date_lo_acceptance",
"dif_acc_date_lo_acceptance",
"Facility Agreement Date",
"acc_facility_agreement_date",
"dif_acc_facility_agreement_date",
"Expiry of Availability Period",
"acc_availability_period",
"dif_acc_availability_period",
"Maturity/Expired Date",
"acc_maturity_expired_date",
"dif_acc_maturity_expired_date",
"Tenure (Month)",
"acc_tenure",
"dif_acc_tenure",
"Operation Country",
"cif_operation_country",
"dif_cif_operation_country",
"Country Exposure",
"facility_country_id",
"dif_facility_country_id",
"State (if Country Exposure is Malaysia)",
"Country Rating",
"acc_country_rating",
"dif_acc_country_rating",
"Region",
"acc_region",
"dif_acc_region",
"Market Type",
"market_type",
"dif_market_type",
"Classification of Entity / Customer Type",
"classification_cust_type",
"dif_classification_cust_type",
"Entity / Customer Type",
"cif_cust_type",
"dif_cif_cust_type",
"Classification of Residency Status",
"classification_residency_status",
"dif_classification_residency_status",
"Main Residency Status",
"classificationResidencyStatus",
"dif_classificationResidencyStatus",
"Residency Status",
"cif_residency_status",
"dif_cif_residency_status",
"Corporate Type",
"cif_corporate_type",
"dif_cif_corporate_type",
"Corporate Status",
"cif_corporate_status",
"dif_cif_corporate_status",
"Justification on Corporate Status ",
"justification_corporate_status",
"dif_justification_corporate_status",
"CCPT Classification",
"ccpt_classification",
"dif_ccpt_classification",
"Rating at Origination",
"acc_rating_origination",
"dif_acc_rating_origination",
"Internal Credit Rating (PD/PF)",
"internal_credit_rating",
"dif_internal_credit_rating",
"CRMS Obligor Risk Rating",
"crms_obligator_risk_rating",
"dif_crms_obligator_risk_rating",
"CRMS CG Rating",
"crms_cg_rating",
"dif_crms_cg_rating",
"PD (%)",
"pd_percent",
"dif_pd_percent",
"LGD (%)",
"lgd_percent",
"dif_lgd_percent",
"MFRS9 Staging",
"acc_MFRS9_staging",
"dif_acc_MFRS9_staging",
"BNM Main Sector",
"bnm_main_sector",
"dif_bnm_main_sector",
"BNM Sub Sector",
"bnm_sub_sector",
"dif_bnm_sub_sector",
"Oil and Gas (Y/N)",
"oil_and_gas_desc",
"dif_oil_and_gas_desc",
"Oil and Gas Segmentation\nDropdown - (Upstream, Midstream, Downstream)",
"oil_and_gas_segmentation",
"dif_oil_and_gas_segmentation",
"Annual Review Date",
"acc_review_date",
"dif_acc_review_date",
"Watchlist Review Date",
"acc_watchlist_review_date_approval",
"dif_acc_watchlist_review_date_approval",
"Payment/Repayment Frequency (Profit/Interest)",
"acc_payment_frequency_interest",
"dif_acc_payment_frequency_interest",
"Payment/Repayment Frequency (Cost/Principal)",
"acc_payment_frequency_principal",
"dif_acc_payment_frequency_principal",
"Effective cost of borrowings",
"acc_effective_cost_borrowings",
"dif_acc_effective_cost_borrowings",
"Profit/Interest Margin",
"acc_margin",
"dif_acc_margin",
"Effective Interest Rate (EIR)",
"acc_average_interest_rate",
"dif_acc_average_interest_rate",
"Ta`widh Compensation/Penalty Rate",
"acc_tadwih_compensation",
"dif_acc_tadwih_compensation",
"Date of Approval Restructured / Rescheduled",
"dateapp_date",
"dif_dateapp_date",
"Effective Date ( R&R )",
"dateapp_effectivedate",
"dif_dateapp_effectivedate",
"Reason Restructured & Resheduled",
"dateapp_reason",
"dif_dateapp_reason",
"Date Untagged from R&R",
"date_untagged_rr",
"dif_date_untagged_rr",
"Justification for Untagged",
"justification_untagged",
"dif_justification_untagged",
"Frequency of R&R ",
"frequency_rr",
"dif_frequency_rr",
"Grace Period (Month)",
"acc_grace_period",
"dif_acc_grace_period",
"Moratorium Period (Month) ",
"moratorium_period_month",
"dif_moratorium_period_month",
"Start Moratorium Date",
"moratorium_start_date",
"dif_moratorium_start_date",
"Date of Overdue",
"acc_date_overdue",
"dif_acc_date_overdue",
"Overdue (Days)",
"acc_overdue_days",
"dif_acc_overdue_days",
"Month in Arrears",
"int_month_in_arrears",
"dif_int_month_in_arrears",
"Overdue Amount (Facility Currency)",
"acc_overdue_ori",
"dif_acc_overdue_ori",
"Overdue Amount (MYR)",
"acc_overdue_amount_myr",
"dif_acc_overdue_amount_myr",
"Date Classified as Watchlist",
"acc_watchlist_date",
"dif_acc_watchlist_date",
"Watchlist Reason",
"acc_watchlist_reason",
"dif_acc_watchlist_reason",
"Date Declassified from Watchlist",
"acc_date_delist_watchlist",
"dif_acc_date_delist_watchlist",
"Date Impaired",
"acc_date_impaired",
"dif_acc_date_impaired",
"Reason for Impairment",
"acc_reason_impairment",
"dif_acc_reason_impairment",
"Partial Write off Date",
"acc_partial_writeoff_date",
"dif_acc_partial_writeoff_date",
"Write off Date",
"acc_writeoff_date",
"dif_acc_writeoff_date",
"Cancellation Date/Fully Settled Date",
"acc_cancel_fulltsettle_date",
"dif_acc_cancel_fulltsettle_date",
"Type of Collateral",
"Collateral Amount ",
"Relationship Manager (RM)",
"acc_relationship_manager_rm",
"dif_acc_relationship_manager_rm",
"Team",
"acc_banking_team",
"dif_acc_banking_team",
"Position as At",
"position_as_at",
"dif_position_as_at"
]]

COMBINE1.to_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\02. MIS Validation\\Test 20250525 _apr2025 Validation loanDatabaseReport_25052025_175249.xlsx", sheet_name="LDB Validation", index=False)
#COMBINE._merge.value_counts()
#COMBINE.iloc[np.where(COMBINE._merge=="left_only")][["EXIM Account Number"]].value_counts()

#COMBINE.dif_cif_number.value_counts()
#COMBINE.iloc[np.where(COMBINE["dif_cif_number"]=="FALSE")][["CIF Number","cif_number","dif_cif_number"]]#[["cif_number"]].value_counts()