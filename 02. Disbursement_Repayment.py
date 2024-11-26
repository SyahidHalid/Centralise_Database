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
    <h1>Disbursement and Repayment</h1>
</div>
"""
st.markdown(html_template, unsafe_allow_html=True)
#st.header('asd')

st.subheader("Start:")
#----------------------------Title--------------------------------------------------------------------

#st.write('# Income Statement')
st.write('Please fill in the form below to auto run by uploading latest file received in xlsx format below:')

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
#Guna ni kalau nk ad submit button
#form = st.form("Basic form")

#name = form.text_input("Name")
#date_format = form.text_input("Input Date (i.e. 202409):")
#age = form.slider("Age", min_value=18, max_value=100, step=1)
#date = form.date_input("Date", value=dt.date.today())

#year = form.slider
year = st.slider("Year", min_value=2020, max_value=2030, step=1)

#month = form.slider
month = st.slider("Month", min_value=1, max_value=12, step=1)

#D1 = form
D1 = st.text_input("Input Islamic Disbursement Sheet ")
D2 = st.text_input("Input Islamic Repayment Sheet ")
D3 = st.text_input("Input Conventional Disbursement Sheet ")
D4 = st.text_input("Input Conventional Repayment Sheet ")


df1 = st.file_uploader(label= "Upload Latest Disbursement Repayment:")

if df1:
  Dis_isl = pd.read_excel(df1, sheet_name=D1, header=8)
  Rep_Isl = pd.read_excel(df1, sheet_name=D2, header=8)
  Dis_Conv = pd.read_excel(df1, sheet_name=D3, header=8)
  Rep_Conv = pd.read_excel(df1, sheet_name=D4, header=8)

df2 = st.file_uploader(label= "Upload Loan Database:")

if df2:
  LDB_prev = pd.read_excel(df2, sheet_name="Loan Database", header=1)
  #st.write(LDB_prev.head(1))

#klau submit form 
#submitted = form.form_submit_button("Submit")
#if submitted:
  #st.write("Submitted")
  #st.write(year, month)

  st.write(f"File submitted for : "+str(year)+"-"+str(month))
  #st.write(f"All file submitted for :{str(year)+str(month)}")
  
  #---------------------------------Start

  Dis_isl.columns = Dis_isl.columns.str.replace("\n", "_")
  Dis_isl.columns = Dis_isl.columns.str.replace(" ", "")

  Rep_Isl.columns = Rep_Isl.columns.str.replace("\n", "_")
  Rep_Isl.columns = Rep_Isl.columns.str.replace(" ", "")

  Dis_Conv.columns = Dis_Conv.columns.str.replace("\n", "_")
  Dis_Conv.columns = Dis_Conv.columns.str.replace(" ", "")

  Rep_Conv.columns = Rep_Conv.columns.str.replace("\n", "_")
  Rep_Conv.columns = Rep_Conv.columns.str.replace(" ", "")

  Dis_isl['St'] =   Dis_isl['St'].str[8:]
  Rep_Isl['St'] =   Rep_Isl['St'].str[8:]
  Dis_Conv['St'] =   Dis_Conv['St'].str[8:]
  Rep_Conv['St'] =   Rep_Conv['St'].str[8:]

  Dis_isl.rename(columns={'St':'Account'},inplace=True)
  Rep_Isl.rename(columns={'St':'Account'},inplace=True)
  Dis_Conv.rename(columns={'St':'Account'},inplace=True)
  Rep_Conv.rename(columns={'St':'Account'},inplace=True)



  Dis_isl_1 = Dis_isl.iloc[np.where((Dis_isl['Unnamed:1']=="**")&(~Dis_isl.Account.isin(['Account']))&~(Dis_isl.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()
  Rep_Isl_1 = Rep_Isl.iloc[np.where((Rep_Isl['Unnamed:1']=="**")&(~Rep_Isl.Account.isin(['Account']))&~(Rep_Isl.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()
  Dis_Conv_1 = Dis_Conv.iloc[np.where((Dis_Conv['Unnamed:1']=="**")&(~Dis_Conv.Account.isin(['Account']))&~(Dis_Conv.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()
  Rep_Conv_1 = Rep_Conv.iloc[np.where((Rep_Conv['Unnamed:1']=="**")&(~Rep_Conv.Account.isin(['Account']))&~(Rep_Conv.Account.isna()))].fillna(0).groupby(['Account','Curr.'])[['Amtinloc.cur.','AmountinDC']].sum().reset_index()

  Dis_isl_1['Type_of_Financing'] = 'Islamic'
  Rep_Isl_1['Type_of_Financing'] = 'Islamic'
  Dis_Conv_1['Type_of_Financing'] = 'Conventional'
  Rep_Conv_1['Type_of_Financing'] = 'Conventional'
  
  #st.write(Dis_isl_1)
  
  Disbursement = pd.concat([Dis_isl_1,Dis_Conv_1])
  Repayment = pd.concat([Rep_Isl_1,Rep_Conv_1])

  Disbursement.rename(columns={'AmountinDC': 'Disbursement_Drawdown_Facility_Currency',
                             'Amtinloc.cur.':'Disbursement_Drawdown_MYR'},inplace=True)

  Repayment.rename(columns={'AmountinDC': 'Cost_Payment_Principal_Repayment_Facility_Currency',
                          'Amtinloc.cur.':'Cost_Payment_Principal_Repayment_MYR'},inplace=True)

  Repayment['Cost_Payment_Principal_Repayment_Facility_Currency'] = -1*Repayment['Cost_Payment_Principal_Repayment_Facility_Currency']
  Repayment['Cost_Payment_Principal_Repayment_MYR'] = -1*Repayment['Cost_Payment_Principal_Repayment_MYR']

  merge = Disbursement.fillna(0).merge(Repayment.fillna(0),on=['Account','Curr.','Type_of_Financing'],how='outer')
  merge.fillna(0, inplace=True)

  merge['Account'] = merge['Account'].astype(str)
  
  #------------------------------------------------Loan Database--------------------------------------
  current_time = pd.Timestamp.now()
  st.write("Current Time: ", current_time)

  import pyodbc
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
    
    df = pd.read_sql_query("select finance_sap_number, acc_drawdown_fc,acc_drawdown_myr,acc_cumulative_drawdown,acc_cumulative_drawdown_myr,acc_repayment_fc,acc_repayment_myr,acc_cumulative_repayment_myr from col_facilities_application_master", conn)
    #df = pd.read_sql_query("select TOP 10 * from col_facilities_application_master", conn)
    
    st.write(df.head())
    st.write(df.shape)

  except pyodbc.Error as ex:
    print('Failed',ex)
  
  
  
  #------------------------------------------------Loan Database--------------------------------------
  
  #dlm db finance_sap_number
  #dlm db acc_cumulative_drawdown
  #dlm db acc_drawdown_fc

  LDB_prev['Finance(SAP) Number'] = LDB_prev['Finance(SAP) Number'].astype(str)

  LDB_prev.columns = LDB_prev.columns.str.replace("\n", "")
  #LDB_prev.fillna(0, inplace=True)

  LDB_prev['Cumulative Disbursement/Drawdown (Facility Currency)'].fillna(0,inplace=True)
  LDB_prev['Cumulative Disbursement/Drawdown (MYR)'].fillna(0,inplace=True)
  LDB_prev['Cumulative Cost Payment/Principal Repayment (Facility Currency)'].fillna(0,inplace=True) 
  LDB_prev['Cumulative Cost Payment/Principal Repayment (MYR)'].fillna(0,inplace=True)

  #st.write(Rep_Isl_1)
           
  appendfinal_ldb = merge.merge(LDB_prev.iloc[np.where(~(LDB_prev['EXIM Account No.'].isna())&(LDB_prev['EXIM Account No.']!="Total"))][['Finance(SAP) Number','EXIM Account No.','CIF Number',
                                              'Customer Name','Facility Currency',
                                              'Cumulative Disbursement/Drawdown (Facility Currency)',
                                              'Cumulative Disbursement/Drawdown (MYR)',
                                              'Cumulative Cost Payment/Principal Repayment (Facility Currency)',
                                              'Cumulative Cost Payment/Principal Repayment (MYR)']].drop_duplicates('Finance(SAP) Number',keep='first').rename(columns={'Finance(SAP) Number':'Account'}),on=['Account'],how='outer', suffixes=('_x', ''),indicator=True)
  #st.write(appendfinal_ldb)

  #---------------------------------------------------------------------------------------------------

  appendfinal_ldb['Disbursement_Drawdown_Facility_Currency'].fillna(0,inplace=True)
  appendfinal_ldb['Disbursement_Drawdown_MYR'].fillna(0,inplace=True)
  appendfinal_ldb['Cost_Payment_Principal_Repayment_Facility_Currency'].fillna(0,inplace=True) 
  appendfinal_ldb['Cost_Payment_Principal_Repayment_MYR'].fillna(0,inplace=True)

  appendfinal_ldb['Cumulative Disbursement/Drawdown (Facility Currency)'].fillna(0,inplace=True)
  appendfinal_ldb['Cumulative Disbursement/Drawdown (MYR)'].fillna(0,inplace=True)
  appendfinal_ldb['Cumulative Cost Payment/Principal Repayment (Facility Currency)'].fillna(0,inplace=True) 
  appendfinal_ldb['Cumulative Cost Payment/Principal Repayment (MYR)'].fillna(0,inplace=True)

  appendfinal2 = appendfinal_ldb#.fillna(0)

  appendfinal2['Cumulative Disbursement/Drawdown (Facility Currency) New'] = appendfinal2['Disbursement_Drawdown_Facility_Currency'] +  appendfinal2['Cumulative Disbursement/Drawdown (Facility Currency)'] 
  appendfinal2['Cumulative Disbursement/Drawdown (MYR) New'] = appendfinal2['Disbursement_Drawdown_MYR'] +  appendfinal2['Cumulative Disbursement/Drawdown (MYR)'] 

  appendfinal2['Cumulative Cost Payment/Principal Repayment (Facility Currency) New'] = appendfinal2['Cost_Payment_Principal_Repayment_Facility_Currency'] +  appendfinal2['Cumulative Cost Payment/Principal Repayment (Facility Currency)'] 
  appendfinal2['Cumulative Cost Payment/Principal Repayment (MYR) New'] = appendfinal2['Cost_Payment_Principal_Repayment_MYR'] +  appendfinal2['Cumulative Cost Payment/Principal Repayment (MYR)'] 

  appendfinal2.sort_values('Disbursement_Drawdown_MYR', ascending=False, inplace=True)

  appendfinal3 = appendfinal2[['CIF Number','EXIM Account No.','Account',
  #'Curr.',
  'Customer Name','Facility Currency',
  'Type_of_Financing',
  'Disbursement_Drawdown_Facility_Currency',
  'Disbursement_Drawdown_MYR',
  'Cumulative Disbursement/Drawdown (Facility Currency) New',
  'Cumulative Disbursement/Drawdown (MYR) New',
  'Cost_Payment_Principal_Repayment_Facility_Currency',
  'Cost_Payment_Principal_Repayment_MYR',
  'Cumulative Cost Payment/Principal Repayment (Facility Currency) New',
  'Cumulative Cost Payment/Principal Repayment (MYR) New']]

  
  #---------------------------------------------Download-------------------------------------------------------------

  pd.set_option("display.max_columns", None) 
  pd.set_option("display.max_colwidth", 1000) #huruf dlm column
  pd.set_option("display.max_rows", 100)
  pd.set_option("display.precision", 2) #2 titik perpuluhan

  #st.write('Sum Total Loans Outstanding (MYR) : RM'+str(sum))
  st.write("")
  #st.write(f"Sum Disbursement Drawdown (FC) : ${float(sum(appendfinal3['Disbursement_Drawdown_Facility_Currency']))}")
  st.write(f"Sum Disbursement Drawdown (MYR) : RM{float(sum(appendfinal3['Disbursement_Drawdown_MYR']))}")
  st.write("")
  #st.write(f"Sum Cumulative Disbursement Drawdown (FC) : ${float(sum(appendfinal3['Cumulative Disbursement/Drawdown (Facility Currency) New']))}")
  st.write(f"Sum Cumulative Disbursement Drawdown (MYR) : RM{float(sum(appendfinal3['Cumulative Disbursement/Drawdown (MYR) New']))}")
  st.write("")
  #st.write(f"Sum Cost Payment Principal Payment (FC) : ${float(sum(appendfinal3['Cost_Payment_Principal_Repayment_Facility_Currency']))}")
  st.write(f"Sum Cost Payment Principal Payment (MYR) : RM{float(sum(appendfinal3['Cost_Payment_Principal_Repayment_MYR']))}")
  st.write("")
  #st.write(f"Sum Cumulative Cost Payment Principal Payment (FC) : ${float(sum(appendfinal3['Cumulative Cost Payment/Principal Repayment (Facility Currency) New']))}")
  st.write(f"Sum Cumulative Cost Payment Principal Payment (MYR) : RM{float(sum(appendfinal3['Cumulative Cost Payment/Principal Repayment (MYR) New']))}")
  
  st.write("")
  st.write("Row Column Checking: ")
  st.write(appendfinal3.shape)
           
  #st.write("SAP Duplication Checking: ")
  #st.write(appendfinal3['Account'].value_counts())

  st.write(appendfinal3)

  st.write("Download file: ")
  st.download_button("Download CSV",
                   appendfinal3.to_csv(index=False),
                   file_name='02. Disbursement Repayment '+str(year)+"-"+str(month)+'.csv',
                   mime='text/csv')