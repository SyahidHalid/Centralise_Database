# import pandas as pd
# import pyodbc
# from sqlalchemy import create_engine
 
# # Database connection settings
# server = '10.20.1.27,1455'
# database = 'ecis'
# username = 'ecis_admin'
# password = 'Exim1234'
# driver = 'ODBC Driver 17 for SQL Server'  # Ensure this is installed
 
# # Create SQLAlchemy engine for bulk insert
# engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")
 
# # Load Excel data
# file_path = r'C:\Users\mafifmsabi\Downloads\test.xlsx'  # Update with your file path
# df = pd.read_excel(file_path, engine='openpyxl')
 
# # Map Excel columns to DB columns
# df_mapped = df.rename(columns={
#     'reference_number': 'reference_number',
#     'decision_desc': 'decision_remark',
#     'cur_date': 'decision_date',
#     'rm_id': 'decision_by'  # Assuming RM ID as username
# })
 
# # Select required columns
# df_mapped = df_mapped[['reference_number', 'decision_remark', 'decision_date', 'decision_by']]
 
# # Insert into SQL Server
# try:
#     df_mapped.to_sql('cla_decision', con=engine, if_exists='append', index=False)
#     print("Data inserted successfully!")
# except Exception as e:
#     print(f"Error: {e}")


# Awang SQL BG

# Select 
# a.companyName as [Borrower]
# ,b.param_name as [Guarantee Type at EXIM]
# ,a.beneficiaryOI as [Beneficiary]
# ,a.counterIssuingBankOI as [Counter Issuing Bank]
# ,c.param_name as [Guarantee Type at Counter Issuing Bank]
# ,d.param_name as [Country]
# ,a.tradeReferenceNo as [Guarantee No.]
# ,f.ConventionalIslamicDesc as [C/I]
# ,a.otherFacilityOI as [Other Facility]
# ,a.currencyOI as [Currency]
# ,a.amountIssuedOI as [Amount Issued]
# ,a.drawdownDate as [Date Issued]
# ,a.maturityDate as [Original Expiry Date]
# ,a.extendedExpiryDate as [Extended Expiry Date]
# ,a.tradeRemark as [Remarks]
# ,a.amountApproved as [Facility Limit]
# ,'' as [Exposure (RM)]
# ,f.acc_undrawn_amount_banking_ori as [Facility Limit Undrawn (FC)]
# ,f.acc_undrawn_amount_myr as [Facility Limit Undrawn (MYR)]
# ,f.CCPT as [CCPT]
# from tradeMaster a
# left outer join param_system_param b on a.guarateeTypeOI = b.param_code and b.parent_param_id = (Select param_id from param_system_param where param_code = 'R118')
# left outer join param_system_param c on a.guaranteeTypeCounterIssuingBankOI = c.param_code and c.parent_param_id = (Select param_id from param_system_param where param_code = 'R119')
# left outer join param_system_param d on a.countryOI = d.param_code and d.parent_param_id = (Select param_id from param_system_param where param_code = 'RR7')
# left outer join col_facilities_application_master e on a.accountNo = e.facility_exim_account_num
 
# left outer join (Select a.facility_exim_account_num
# ,d.param_name as ConventionalIslamicDesc
# ,a.acc_total_banking_exposure_myr
# ,a.acc_undrawn_amount_banking_ori
# ,a.acc_undrawn_amount_myr
# ,e.param_name as CCPT
# ,ROW_NUMBER() OVER (PARTITION BY a.facility_exim_account_num order by c.application_date desc) as num
# from col_facilities_application_master a 
# left outer join account_to_application b on a.facility_id = b.facility_id
# left outer join application_master c on b.application_id = c.application_id
# left outer join param_system_param d on c.financing_type = d.param_id
# left outer join param_system_param e on c.CCPT_classification = e.param_id) f on a.accountNo = f.facility_exim_account_num and num = 1
# where a.tradeMethod = 'BG'