import pyodbc
import pandas as pd
import streamlit as st
import numpy as np

st.set_page_config(
  page_title = 'Loan Database - Automation',
  page_icon = "EXIM.png",
  layout="wide"
  )

html_template = """
<div style="display: flex; align-items: center;">
    <img src="https://www.exim.com.my/wp-content/uploads/2022/07/video-thumbnail-preferred-financier.png" alt="EXIM Logo" style="width: 200px; height: 72px; margin-right: 10px;">
    <h1>Finance</h1>
</div>
"""
st.markdown(html_template, unsafe_allow_html=True)

current_time = pd.Timestamp.now()

st.subheader("Start:")

st.write('Debtor Listing: Please fill in the form below to auto run by uploading latest file received in xlsx format below:')

year = st.slider("Year", min_value=2020, max_value=2030, step=1)

month = st.slider("Month", min_value=1, max_value=12, step=1)

D1 = st.text_input("Input Islamic Cost Sheet ")
D2 = st.text_input("Input Islamic Profit Sheet ")
D3 = st.text_input("Input Mora Sheet ")
D4 = st.text_input("Input Conventional Cost Sheet ")
D5 = st.text_input("Input Conventional Accrued Sheet ")
D6 = st.text_input("Input Conventional Other Charges Sheet ")
D7 = st.text_input("Input Islamic Other Charges Sheet ")
D8 = st.text_input("Input IIS Sheet ")
D9 = st.text_input("Input PIS Sheet ")

BalanceOS = st.text_input("Input Balance Column ") #"Balance_31stAugust2024"

df1 = st.file_uploader(label= "Upload Latest Debtor Listing:")

if df1:
  Isl_Cost = pd.read_excel(df1, sheet_name=D1, header=5)
  Isl_Profit = pd.read_excel(df1, sheet_name=D2, header=3)
  Mora = pd.read_excel(df1, sheet_name=D3, header=5)
  Conv = pd.read_excel(df1, sheet_name=D4, header=2)
  Accrued = pd.read_excel(df1, sheet_name=D5, header=4)
  Others_conv = pd.read_excel(df1, sheet_name=D6, header=4)
  Others_Isl = pd.read_excel(df1, sheet_name=D7, header=4)
  IIS = pd.read_excel(df1, sheet_name=D8, header=4)
  PIS = pd.read_excel(df1, sheet_name=D9, header=4)

  #---------------------------------Debtors Listing Islamic (Cost) include adjustment

  Isl_Cost1 = Isl_Cost.iloc[np.where(~Isl_Cost['Customer\nAccount'].isna())]
    
  Isl_Cost1.columns = Isl_Cost1.columns.str.replace("\n", "_")
  Isl_Cost1.columns = Isl_Cost1.columns.str.replace(" ", "")

  Isl_Cost1.Customer_Account = Isl_Cost1.Customer_Account.astype(int)
  Isl_Cost1.Disbursement = Isl_Cost1.Disbursement.astype(float)
  Isl_Cost1.Cost_Payment = Isl_Cost1.Cost_Payment.astype(float)
  Isl_Cost1[BalanceOS] = Isl_Cost1[BalanceOS].astype(float)

  Isl_Cost1.rename(columns={"Disbursement":"Disbursement - old"}, inplace=True)
  Isl_Cost1['Disbursement'] = Isl_Cost1['Disbursement - old'].fillna(0) + Isl_Cost1['Adjustment/_Capitalisation'].fillna(0)

  Isl_Cost1.rename(columns={"Cost_Payment":"Cost_Payment - old"}, inplace=True)
  Isl_Cost1['Cost_Payment'] = Isl_Cost1['Cost_Payment - old'].fillna(0) - Isl_Cost1['Adjustment/_Capitalisation.1'].fillna(0)

  Isl_Cost2 = Isl_Cost1.fillna(0).groupby(['Company','Customer_Account'\
  ,'Currency'])[['Disbursement'\
  ,'Cost_Payment',BalanceOS]].sum().reset_index()

  Isl_Cost2 = Isl_Cost2.rename(columns={BalanceOS: 'Principal'}).fillna(0).sort_values(by=['Principal'],ascending=[True])

  Isl_Cost2['Financing_Type'] = 'Islamic'

  st.write(Isl_Cost2)

  #---------------------------------Database

st.write("Current Time: ", current_time)

try:
    conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"+
                        "Server=10.32.1.51,1455;"+
                        "Database=mis_db_prod_backup_2024_04_02;"+
                        "Trusted_Connection=no;"+
                        "uid=mis_admin;"+
                        "pwd=Exim1234")
    cursor = conn.cursor()
    #cursor.execute('SELECT TOP 10 * FROM col_facilities_application_master')
    #for row in cursor:
    #    print('row = %r' % (row,))
   
    df = pd.read_sql_query("select TOP 10 * from col_facilities_application_master", conn)
    
    st.write(df.head())


except pyodbc.Error as ex:
    print('Failed',ex)


#++++++++++++++++++++++++++++++++

# Testing SQL

SELECT param_name,
		r.exchange_rate,
		r.valuedate
  FROM [param_ccy_exchange_rate] r inner join param_system_param p 
  on p.param_reference ='Root>>Currency' and currency_id=p.param_id  
  where valuedate =  (SELECT max((valuedate ))
        FROM [param_ccy_exchange_rate]
        WHERE year(valuedate) >= year(getdate())) 

SELECT param_name,r.exchange_rate,r.valuedate
  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id  
  where valuedate=eomonth(valuedate) 


select   exchange_rate from param_ccy_exchange_rate 
where (SELECT (eomonth(max(valuedate)))
        FROM [param_ccy_exchange_rate]
		WHERE  year(valuedate) >= year(getdate()) )
		
		
		and month(valuedate) >= month(getdate())
#final

SELECT param_name,
		r.exchange_rate,
		r.valuedate FROM [param_ccy_exchange_rate] r inner join param_system_param p 
  on p.param_reference ='Root>>Currency' and currency_id=p.param_id  
  where valuedate =  (SELECT (eomonth(max(valuedate)))
        FROM [param_ccy_exchange_rate]
		WHERE year(valuedate) >= year(getdate()))



with x as (SELECT max(r.valuedate) as le

  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id

  where valuedate=eomonth(valuedate))
 
SELECT param_name,r.exchange_rate,r.valuedate

  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id

  inner join x on x.le = valuedate order by param_name asc


#====================================

#how to insert record with from continue previous number in sqlquery

insert value

#coalesce ug sambung id

INSERT INTO employees (employee_id, name, department)
VALUES (
    (SELECT COALESCE(MAX(employee_id), 0) + 1 FROM employees),
    'John Doe',
    'Sales'
);


INSERT INTO employees (name, department)
VALUES ('John Doe', 'Sales');

CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50)

    INSERT INTO Employees (EmployeeID, FirstName, LastName, Position)
VALUES (1, 'John', 'Doe', 'Manager');
 

 #how to write try if succeed then upload table A failed then upload table B

def upload_table_a():
    # Code to upload Table A
    print("Uploading Table A...")
    # Simulate a success or failure
    success = True  # Set to False to simulate failure
    if not success:
        raise Exception("Failed to upload Table A")
    print("Table A uploaded successfully.")

def upload_table_b():
    # Code to upload Table B
    print("Uploading Table B...")
    # Simulate the success of Table B upload
    print("Table B uploaded successfully.")

try:
    upload_table_a()  # Try to upload Table A
except Exception as e:
    print(f"Error: {e}")
    print("Uploading Table B instead...")
    upload_table_b()  # If Table A fails, upload Table B

