import streamlit as st
import pandas as pd
import numpy as np
#import base64
#from PIL import Image
#import plotly.express as px

#warnings.filterwarnings('ignore')
#pd.set_option("display.max_columns", None) 
#pd.set_option("display.max_colwidth", 1000) #huruf dlm column
#pd.set_option("display.max_rows", 100)
#pd.set_option("display.precision", 2) #2 titik perpuluhan

#----------------------nama kat web atas yg newtab (png sahajer)--------------------
st.set_page_config(
  page_title = 'Loan Database - Automation',
  page_icon = "EXIM.png",
  layout="wide"
  )

#to show code kat website

#with st.echo():
#  def sum(a, b):
#    return a + b

#----------------------header
html_template = """
<div style="display: flex; align-items: center;">
    <img src="https://www.exim.com.my/wp-content/uploads/2022/07/video-thumbnail-preferred-financier.png" alt="EXIM Logo" style="width: 200px; height: 72px; margin-right: 10px;">
    <h1>CCRIS Uploading</h1>
</div>
"""
st.markdown(html_template, unsafe_allow_html=True)
#st.header('asd')
st.subheader("Reminder to copy data to template before start:")
#----------------------------Title--------------------------------------------------------------------

#st.write('# Income Statement')
st.write('Please fill in the form below to auto run by uploading latest loan database received in xlsx format below:')

#----------------------------Input--------------------------------------------------------------------
#X = st.text_input("Input Date (i.e. 202409):")
#Y = st.text_input("Input Name (i.e. 09. Income statement Sep 2024):")

# klau nk user isi dlu bru boleh forward
#if not X:
#  st.warning("Enter Date!")
#  st.stop()
#st.success("Go ahead")

#if not Y:
#  st.warning("Enter Name!")
#  st.stop()
#st.success("Go ahead")

#----------------------------Form--------------------------------------------------------------------

form = st.form("Basic form")
#name = form.text_input("Name")

#date_format = form.text_input("Input Date (i.e. 202409):")

year = form.slider("Year", min_value=2020, max_value=2030, step=1)
month = form.slider("Month", min_value=1, max_value=12, step=1)
#sheet = form.text_input("Input sheet Name ")
sheet = "Loan Database"

#age = form.slider("Age", min_value=18, max_value=100, step=1)
#date = form.date_input("Date", value=dt.date.today())

df1 = form.file_uploader(label= "Upload Latest Loan Database:")

if df1:
  df1 = pd.read_excel(df1, sheet_name=sheet, header=1)
  #st.write(df1.head(1))

submitted = form.form_submit_button("Submit")
if submitted:
  #st.write("Submitted")
  #st.write(year, month)

  st.write(f"File submitted for : "+str(year)+"-"+str(month))
  #st.write(f"All file submitted for :{str(year)+str(month)}")

  LDB1 = df1.iloc[np.where(~df1['CIF Number'].isna())]
  
  #st.write(LDB1.head(3))

  LDB1.columns = LDB1.columns.str.strip()
  LDB1.columns = LDB1.columns.str.replace("\n", "")
  
  LDB1['LGD'] = ""
  LDB1['Risk Category'] = ""
  LDB1['Prudential Limit (%)'] = ""
  LDB1["EXIMs Shareholder Fund as at"] = ""
  LDB1["EXIMs Shareholder Fund as at (MYR)"] = ""
  LDB1['Single Customer Exposure Limit (SCEL)(MYR)'] = ""
  LDB1['Percentage of Total Banking Exposure(MYR) to SCEL (MYR)'] = ""
  LDB1['Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)'] = ""
  LDB1['EXIM Main Sector'] = ""
  LDB1['SME Commercial Corporate'] = ""
  LDB1['PF'] = ""
  LDB1['Risk Analyst'] = ""
  LDB1['Ownership'] = ""
  LDB1['Officer in Charge'] = ""

  LDB2 = LDB1[['CIF Number',
             'EXIM Account No.',
                     'Application System Code',
                     'CCRIS Master Account Number',
                     'CCRIS Sub Account Number',
                            'Finance(SAP) Number',
                            'Company Group',
                            'Customer Name',
                            'Relationship Manager (RM)',
                            'Team', #Banking Team',
                            'Ownership',
                            'Officer in Charge',
                            'Syndicated / Club Deal',
                            'Nature of Account',
                            'Facility',
                            'Facility Currency',
                            'Type of Financing',
                            'Shariah Contract / Concept',
                            'Status',
                            'Post Approval Stage',
                            'Date of Ready for Utilization (RU)',
                            #'Restructured / Rescheduled(Y/N)',
                            'Amount Approved / Facility Limit (Facility Currency)',
                            'Amount Approved / Facility Limit (MYR)',
                            'Cost/Principal Outstanding (Facility Currency)',
                            'Cost/Principal Outstanding (MYR)',
                            'Contingent Liability Letter of Credit (Facility Currency)',
                            'Contingent Liability Letter of Credit (MYR)',
                            'Contingent Liability (Facility Currency)',
                            'Contingent Liability (MYR)',
                            'Account Receivables/Past Due Claims (Facility Currency)',
                            'Account Receivable/Past Due Claims (MYR)',
                            'Total Banking Exposure (Facility Currency)',
                            'Total Banking Exposure (MYR)',
                            'Accrued Profit/Interest of the month (Facility Currency)',
                            'Accrued Profit/Interest of the month (MYR)',
                            'Modification of Loss (Facility Currency)',
                            'Modification of Loss (MYR)',
                            'Cumulative Accrued Profit/Interest (Facility Currency)',
                            'Cumulative Accrued Profit/Interest (MYR)',                            
                            'Penalty/Ta`widh (Facility Currency)',
                            'Penalty/Ta`widh (MYR)',
                            'Income/Interest in Suspense (Facility Currency)',
                            'Income/Interest in Suspense (MYR)',
                            'Other Charges (Facility Currency)',
                            'Other Charges (MYR)',
                            'Total Loans Outstanding (Facility Currency)',
                            'Total Loans Outstanding (MYR)',
             'Expected Credit Loss (ECL) LAF (Facility Currency)',
             'Expected Credit Loss LAF (ECL) (MYR)',
             'Expected Credit Loss C&C (ECL) (Facility Currency)',
             'Expected Credit Loss C&C (ECL) (MYR)',
                            'Disbursement/Drawdown Status',
                            'Unutilised/Undrawn Amount (Facility Currency)',
                            'Unutilised/Undrawn Amount (MYR)',
                            'Disbursement/Drawdown (Facility Currency)',
                            'Disbursement/Drawdown (MYR)',
                            'Cumulative Disbursement/Drawdown (Facility Currency)',
                            'Cumulative Disbursement/Drawdown (MYR)',
                            'Cost Payment/Principal Repayment (Facility Currency)',
                            'Cost Payment/Principal Repayment (MYR)',                            
                            'Cumulative Cost Payment/Principal Repayment (Facility Currency)',
                            'Cumulative Cost Payment/Principal Repayment (MYR)',
                            'Profit Payment/Interest Repayment (Facility Currency)',
                            'Profit Payment/Interest Repayment (MYR)',
                            'Cumulative Profit Payment/Interest Repayment (Facility Currency)',
                            'Cumulative Profit Payment/Interest Repayment (MYR)',
                            'Ta`widh Payment/Penalty Repayment (Facility Currency)',
                            'Ta`widh Payment/Penalty Repayment  (MYR)',
                            'Cumulative Ta`widh Payment/Penalty Repayment (Facility Currency)',
                            'Cumulative Ta`widh Payment/Penalty Repayment  (MYR)',
                            'Other Charges Payment (Facility Currency)',
                            'Other Charges Payment (MYR)',
                            'Cumulative Other Charges Payment (Facility Currency)',
                            'Cumulative Other Charges Payment (MYR)',
                            'Rating at Origination',
                            'Internal Credit Rating (PD/PF)', #bru  #'PD',
                            'PF',
             'LGD',
                            'CRMS Obligor Risk Rating',
                            #'CRMS CG Rating', 'CCPT Classification',
                            'PD (%)',
                            'LGD (%)',
            'Risk Category',
            "Prudential Limit (%)",
            "EXIMs Shareholder Fund as at",
            "EXIMs Shareholder Fund as at (MYR)",
            "Single Customer Exposure Limit (SCEL)(MYR)",
            "Percentage of Total Banking Exposure(MYR) to SCEL (MYR)",
            "Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)",
                            'Risk Analyst',
                            'MFRS9 Staging',
                            'BNM Main Sector',
                            'BNM Sub Sector',
                            'EXIM Main Sector',
                            'Industry (Risk)',
                            'Industry Classification',
                            'Purpose of Financing',
                            'Date Approved at Origination',
                            'Approval Authority',
                            'LO issuance Date',
                            'Date of LO Acceptance',
                            '1st Disbursement Date / 1st Drawdown Date',
                            '1st Payment/Repayment Date',
                            'Expiry of Availability Period',
                            'Facility Agreement Date',
                            'Annual Review Date',
                            'Watchlist Review Date',
                            'Maturity/Expired Date',
                            'Grace Period (Month)',
                            'Moratorium Period (Month)',
                            'Start Moratorium Date',
        'Fund Type',
                            'Tenure (Month)',
                            'Payment/Repayment Frequency (Profit/Interest)',
                            'Payment/Repayment Frequency (Cost/Principal)',
                            'Effective cost of borrowings',
                            'Profit/Interest Margin',
                            'Effective Interest Rate (EIR)', #bru  #'Average Profit/Interest Rate',
                            'Ta`widh Compensation/Penalty Rate',
                            'Operation Country',
                            'Country Exposure',
                            'Country Rating',
                            'Region',
                            'Market Type',
                            'Classification of Entity / Customer Type',
                            'Entity / Customer Type',
                            'Classification of Residency Status',
                            'Residency Status',#'Main Residency Status',
                            'Corporate Type',
                            'SME Commercial Corporate', #tukar
                            'Corporate Status',
                            'Justification on Corporate Status',
                            'Restructured / Rescheduled',
                            'Date of Approval Restructured / Rescheduled',
                            'Effective Date ( R&R )',
                            'Reason Restructured & Resheduled',
                            #'Date Untagged from R&R',
                            #'Justification for Untagged',
                            'Frequency of R&R',
                            'Date of Overdue',
             'Overdue (Days)',
             'Month in Arrears',
             'Overdue Amount (Facility Currency)',
             'Overdue Amount (MYR)',
                            'Date Classified as Watchlist',
                            'Watchlist Reason',
                            'Date Declassified from Watchlist',
                            'Date Impaired',
                            'Reason for Impairment',
                            'Partial Write off Date',
                            'Write off Date',
                            'Cancellation Date/Fully Settled Date','Position as At']]
  
  LDB2['Finance(SAP) Number'] = LDB2['Finance(SAP) Number'].astype(str)

  LDB2.loc[(LDB2['Finance(SAP) Number'].isin(['BG-I','BG','500724'])),'Expected Credit Loss C&C (ECL) (MYR)'] = 0
  LDB2.loc[(LDB2['Finance(SAP) Number'].isin(['BG-I','BG','500724'])),'Expected Credit Loss C&C (ECL) (Facility Currency)'] = 0

  LDB2['Expected Credit Loss LAF (ECL) (MYR) 2'] = LDB2['Expected Credit Loss LAF (ECL) (MYR)'].fillna(0) + LDB2['Expected Credit Loss C&C (ECL) (MYR)'].fillna(0)
  LDB2['Expected Credit Loss (ECL) LAF (Facility Currency) 2'] = LDB2['Expected Credit Loss (ECL) LAF (Facility Currency)'].fillna(0) + LDB2['Expected Credit Loss C&C (ECL) (Facility Currency)'].fillna(0)

  LDB3 = LDB2.drop(['Expected Credit Loss C&C (ECL) (MYR)','Expected Credit Loss C&C (ECL) (Facility Currency)'],axis=1)
  
  #st.write(LDB2.iloc[np.where(LDB2['Finance(SAP) Number'].isin(['500204']))])

  LDB4 = LDB3[['CIF Number',
             'EXIM Account No.',
                     'Application System Code',
                     'CCRIS Master Account Number',
                     'CCRIS Sub Account Number',
                            'Finance(SAP) Number',
                            'Company Group',
                            'Customer Name',
                            'Relationship Manager (RM)',
                            'Team', #Banking Team',
                            'Ownership',
                            'Officer in Charge',
                            'Syndicated / Club Deal',
                            'Nature of Account',
                            'Facility',
                            'Facility Currency',
                            'Type of Financing',
                            'Shariah Contract / Concept',
                            'Status',
                            'Post Approval Stage',
                            'Date of Ready for Utilization (RU)',
                            #'Restructured / Rescheduled(Y/N)',
                            'Amount Approved / Facility Limit (Facility Currency)',
                            'Amount Approved / Facility Limit (MYR)',
                            'Cost/Principal Outstanding (Facility Currency)',
                            'Cost/Principal Outstanding (MYR)',
                            'Contingent Liability Letter of Credit (Facility Currency)',
                            'Contingent Liability Letter of Credit (MYR)',
                            'Contingent Liability (Facility Currency)',
                            'Contingent Liability (MYR)',
                            'Account Receivables/Past Due Claims (Facility Currency)',
                            'Account Receivable/Past Due Claims (MYR)',
                            'Total Banking Exposure (Facility Currency)',
                            'Total Banking Exposure (MYR)',
                            'Accrued Profit/Interest of the month (Facility Currency)',
                            'Accrued Profit/Interest of the month (MYR)',
                            'Modification of Loss (Facility Currency)',
                            'Modification of Loss (MYR)',
                            'Cumulative Accrued Profit/Interest (Facility Currency)',
                            'Cumulative Accrued Profit/Interest (MYR)',                            
                            'Penalty/Ta`widh (Facility Currency)',
                            'Penalty/Ta`widh (MYR)',
                            'Income/Interest in Suspense (Facility Currency)',
                            'Income/Interest in Suspense (MYR)',
                            'Other Charges (Facility Currency)',
                            'Other Charges (MYR)',
                            'Total Loans Outstanding (Facility Currency)',
                            'Total Loans Outstanding (MYR)',
             'Expected Credit Loss (ECL) LAF (Facility Currency) 2',
             'Expected Credit Loss LAF (ECL) (MYR) 2',
            #             'Expected Credit Loss C&C (ECL) (Facility Currency)',
            #             'Expected Credit Loss C&C (ECL) (MYR)',
                            'Disbursement/Drawdown Status',
                            'Unutilised/Undrawn Amount (Facility Currency)',
                            'Unutilised/Undrawn Amount (MYR)',
                            'Disbursement/Drawdown (Facility Currency)',
                            'Disbursement/Drawdown (MYR)',
                            'Cumulative Disbursement/Drawdown (Facility Currency)',
                            'Cumulative Disbursement/Drawdown (MYR)',
                            'Cost Payment/Principal Repayment (Facility Currency)',
                            'Cost Payment/Principal Repayment (MYR)',                            
                            'Cumulative Cost Payment/Principal Repayment (Facility Currency)',
                            'Cumulative Cost Payment/Principal Repayment (MYR)',
                            'Profit Payment/Interest Repayment (Facility Currency)',
                            'Profit Payment/Interest Repayment (MYR)',
                            'Cumulative Profit Payment/Interest Repayment (Facility Currency)',
                            'Cumulative Profit Payment/Interest Repayment (MYR)',
                            'Ta`widh Payment/Penalty Repayment (Facility Currency)',
                            'Ta`widh Payment/Penalty Repayment  (MYR)',
                            'Cumulative Ta`widh Payment/Penalty Repayment (Facility Currency)',
                            'Cumulative Ta`widh Payment/Penalty Repayment  (MYR)',
                            'Other Charges Payment (Facility Currency)',
                            'Other Charges Payment (MYR)',
                            'Cumulative Other Charges Payment (Facility Currency)',
                            'Cumulative Other Charges Payment (MYR)',
                            'Rating at Origination',
                            'Internal Credit Rating (PD/PF)',  #'PD',
                            'PF',
             'LGD',
                            'CRMS Obligor Risk Rating',
                            #'CRMS CG Rating', 'CCPT Classification',
                            'PD (%)',
                            'LGD (%)',
            'Risk Category',
            "Prudential Limit (%)",
            "EXIMs Shareholder Fund as at",
            "EXIMs Shareholder Fund as at (MYR)",
            "Single Customer Exposure Limit (SCEL)(MYR)",
            "Percentage of Total Banking Exposure(MYR) to SCEL (MYR)",
            "Percentage of Total Overall Banking Exposure (MYR) to SCEL (MYR) (%)",
                            'Risk Analyst',
                            'MFRS9 Staging',
                            'BNM Main Sector',
                            'BNM Sub Sector',
                            'EXIM Main Sector',
                            'Industry (Risk)',
                            'Industry Classification',
                            'Purpose of Financing',
                            'Date Approved at Origination',
                            'Approval Authority',
                            'LO issuance Date',
                            'Date of LO Acceptance',
                            '1st Disbursement Date / 1st Drawdown Date',
                            '1st Payment/Repayment Date',
                            'Expiry of Availability Period',
                            'Facility Agreement Date',
                            'Annual Review Date',
                            'Watchlist Review Date',
                            'Maturity/Expired Date',
                            'Grace Period (Month)',
                            'Moratorium Period (Month)',
                            'Start Moratorium Date',
             'Fund Type',
                            'Tenure (Month)',
                            'Payment/Repayment Frequency (Profit/Interest)',
                            'Payment/Repayment Frequency (Cost/Principal)',
                            'Effective cost of borrowings',
                            'Profit/Interest Margin',
                            'Effective Interest Rate (EIR)', #'Average Profit/Interest Rate',
                            'Ta`widh Compensation/Penalty Rate',
                            'Operation Country',
                            'Country Exposure',
                            'Country Rating',
                            'Region',
                            'Market Type',
                            'Classification of Entity / Customer Type',
                            'Entity / Customer Type',
                            'Classification of Residency Status',
                            'Residency Status',#'Main Residency Status',
                            'Corporate Type',
                            'SME Commercial Corporate',
                            'Corporate Status',
                            'Justification on Corporate Status',
                            'Restructured / Rescheduled',
                            'Date of Approval Restructured / Rescheduled',
                            'Effective Date ( R&R )',
                            'Reason Restructured & Resheduled',
                            #'Date Untagged from R&R',
                            #'Justification for Untagged',
                            'Frequency of R&R',
                            'Date of Overdue',
             'Overdue (Days)',
             'Month in Arrears',
             'Overdue Amount (Facility Currency)',
             'Overdue Amount (MYR)',
                            'Date Classified as Watchlist',
                            'Watchlist Reason',
                            'Date Declassified from Watchlist',
                            'Date Impaired',
                            'Reason for Impairment',
                            'Partial Write off Date',
                            'Write off Date',
                            'Cancellation Date/Fully Settled Date',
                            'Position as At']]
  
  LDB4.fillna(0,inplace=True)
  
  #---------------------------------------------Details-------------------------------------------------------------
  
  st.write(LDB4)

  st.write("Column checking: ")
  st.write(LDB4.shape)

  st.write("")
  st.write("Download file: ")
  st.download_button("Download CSV",
                   LDB4.to_csv(index=False),
                   file_name='Loan Database as at '+str(year)+"-"+str(month)+' - CCRIS RAW.csv',
                   mime='text/csv')
  
  #st.write("Amount checking: ")
  #st.write(LDB4.fillna(0).groupby(['Status'])[['Amount Approved / Facility Limit (MYR)',
  #                          'Cost/Principal Outstanding (MYR)',
  #                          'Total Banking Exposure (MYR)',
  #                          'Total Loans Outstanding (MYR)',
  #           'Expected Credit Loss LAF (ECL) (MYR) 2']].sum().reset_index())
  
  #st.write("Account duplicate checking: ")
  #st.write(LDB4['EXIM Account No.'].value_counts())

  #st.write(LDB4.iloc[np.where(LDB4['EXIM Account No.']==value)])

