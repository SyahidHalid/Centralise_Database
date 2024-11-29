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
    <h1>Allowance</h1>
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

form = st.form("Basic form")
#name = form.text_input("Name")
#date_format = form.text_input("Input Date (i.e. 202409):")

year = form.slider("Year", min_value=2020, max_value=2030, step=1)
month = form.slider("Month", min_value=1, max_value=12, step=1)

#age = form.slider("Age", min_value=18, max_value=100, step=1)
#date = form.date_input("Date", value=dt.date.today())

D1 = form.text_input("Input IA Conventional Sheet ")
D2 = form.text_input("Input IA Islamic Sheet ")
D3 = form.text_input("Input IA IIS Sheet ")
D4 = form.text_input("Input C&C Conventional Sheet ")
D5 = form.text_input("Input C&C Islamic Sheet ")

May_RM = form.text_input("1st (IA) MYR Column Sequence Aug24 is 71, Add 1 for next: Sep24 72")
May_FC = form.text_input("1st (IA) FC Column Sequence Aug24 is 142, Add 2 for next: Sep24 is 144")

May_RM_Is = form.text_input("2nd (C&C) MYR Column Sequence Aug24 is 61, Add 1 for next: Sep24 62")
May_FC_Is = form.text_input("2nd (C&C) FC Column Sequence Aug24 is 122, Add 2 for next: Sep24 is 124")

df1 = form.file_uploader(label= "Upload Latest Allowance:")

if df1:
  IA_Conv = pd.read_excel(df1, sheet_name=D1, header=6)
  IA_Isl = pd.read_excel(df1, sheet_name=D2, header=6)
  IA_IIS = pd.read_excel(df1, sheet_name=D3, header=6)
  CnC_Conv = pd.read_excel(df1, sheet_name=D4, header=6)
  CnC_Isl = pd.read_excel(df1, sheet_name=D5, header=6)

df2 = form.file_uploader(label= "Upload Loan Database:")

if df2:
  LDB_prev = pd.read_excel(df2, sheet_name="Loan Database", header=1)
  #st.write(LDB_prev.head(1))



submitted = form.form_submit_button("Submit")
if submitted:
  #st.write("Submitted")
  #st.write(year, month)

  st.write(f"1st Sequence Checking : "+str(year)+"-"+str(month))
  st.write(IA_Conv.head(1))

  st.write(f"2nd Sequence Checking : "+str(year)+"-"+str(month))
  st.write(CnC_Conv.head(1))

  st.write(f"File submitted for : "+str(year)+"-"+str(month))
  #st.write(f"All file submitted for :{str(year)+str(month)}")
  
  #---------------------------------Start

  IA_Conv.columns = IA_Conv.columns.str.replace("\n", "_")
  IA_Conv.columns = IA_Conv.columns.str.replace(" ", "_")
  IA_Conv.columns = IA_Conv.columns.str.replace(".", "_")

  IA_Isl.columns = IA_Isl.columns.str.replace("\n", "_")
  IA_Isl.columns = IA_Isl.columns.str.replace(" ", "_")
  IA_Isl.columns = IA_Isl.columns.str.replace(".", "_")

  IA_IIS.columns = IA_IIS.columns.str.replace("\n", "_")
  IA_IIS.columns = IA_IIS.columns.str.replace(" ", "_")
  IA_IIS.columns = IA_IIS.columns.str.replace(".", "_")

  CnC_Conv.columns = CnC_Conv.columns.str.replace("\n", "_")
  CnC_Conv.columns = CnC_Conv.columns.str.replace(" ", "_")
  CnC_Conv.columns = CnC_Conv.columns.str.replace(".", "_")

  CnC_Isl.columns = CnC_Isl.columns.str.replace("\n", "_")
  CnC_Isl.columns = CnC_Isl.columns.str.replace(" ", "_")
  CnC_Isl.columns = CnC_Isl.columns.str.replace(".", "_")

  IA_Conv_1 = IA_Conv.iloc[np.where(~(IA_Conv.Loan_Acc_.isna())&~(IA_Conv.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA_'+May_RM,'Closing_'+May_FC]].sum().reset_index()

  IA_Isl_1 = IA_Isl.iloc[np.where((~(IA_Isl.Loan_Acc_.isna()))&~(IA_Isl.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA_'+May_RM,'Closing_'+May_FC]].sum().reset_index()


  IA_IIS.loc[IA_IIS.Borrower=="PT Mahakarya Inti Buana",'Loan_Acc_']='500039'
  IA_IIS['Loan_Acc_'].fillna(0, inplace=True)


  
  IA_IIS_1 = IA_IIS.iloc[np.where((~(IA_IIS.Loan_Acc_==0))&~(IA_IIS.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['IIS_(RM)_'+May_RM,'IIS_(FC)_'+May_RM]].sum().reset_index()

  #IA_IIS_1['Loan_Acc_'] = IA_IIS_1['Loan_Acc_'].astype(int)

  CnC_Conv_1 = CnC_Conv.iloc[np.where((~(CnC_Conv.Loan_Acc_.isna()))&~(CnC_Conv.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA_'+May_RM_Is,'Closing_'+May_FC_Is]].sum().reset_index()

  CnC_Isl_1 = CnC_Isl.iloc[np.where((~(CnC_Isl.Loan_Acc_.isna()))&~(CnC_Isl.Ccy.isna()))].fillna(0).groupby(['Loan_Acc_','Ccy','Borrower'])[['Closing_IA_'+May_RM_Is,'Closing_'+May_FC_Is]].sum().reset_index()
  CnC_Isl_1['Loan_Acc_'] = CnC_Isl_1['Loan_Acc_'].astype(int)

  IA_Conv_1.rename(columns={'Closing_IA_'+May_RM:'LAF_ECL_MYR',
                          'Closing_'+May_FC:'LAF_ECL_FC'},inplace=True)

  IA_Isl_1.rename(columns={'Closing_IA_'+May_RM:'LAF_ECL_MYR',
                          'Closing_'+May_FC:'LAF_ECL_FC'},inplace=True)

  IA_IIS_1.rename(columns={'IIS_(RM)_'+May_RM:'LAF_ECL_MYR',
                         'IIS_(FC)_'+May_RM:'LAF_ECL_FC'},inplace=True)

  CnC_Conv_1.rename(columns={'Closing_IA_'+May_RM_Is:'CnC_ECL_MYR',
                          'Closing_'+May_FC_Is:'CnC_ECL_FC'},inplace=True)

  CnC_Isl_1.rename(columns={'Closing_IA_'+May_RM_Is:'CnC_ECL_MYR',
                          'Closing_'+May_FC_Is:'CnC_ECL_FC'},inplace=True)

  IA_Conv_1['Type_of_Financing'] = 'Conventional'
  IA_Isl_1['Type_of_Financing'] = 'Islamic'
  IA_IIS_1['Type_of_Financing'] = 'Conventional'
  CnC_Conv_1['Type_of_Financing'] = 'Conventional'
  CnC_Isl_1['Type_of_Financing'] = 'Islamic'

  IA_IIS_1.loc[IA_IIS_1.Borrower!="PT Mahakarya Inti Buana",'LAF_ECL_MYR']=0
  IA_IIS_1.loc[IA_IIS_1.Borrower!="PT Mahakarya Inti Buana",'LAF_ECL_FC']=0

  merge = pd.concat([IA_Conv_1,IA_Isl_1,IA_IIS_1,CnC_Conv_1,CnC_Isl_1])

  merge.fillna(0, inplace=True)

  merge['Loan_Acc_'] = merge['Loan_Acc_'].astype(str)
  #mergee['Ccy'] = merge['Ccy'].astype(float)
  #mergee['Borrower'] = merge['Borrower'].astype(float)
  #mergee['Type_of_Financing'] = merge['Type_of_Financing'].astype(float)
  merge['LAF_ECL_FC'] = merge['LAF_ECL_FC'].astype(float)
  merge['LAF_ECL_MYR'] = merge['LAF_ECL_MYR'].astype(float)
  merge['CnC_ECL_FC'] = merge['CnC_ECL_FC'].astype(float)
  merge['CnC_ECL_MYR'] = merge['CnC_ECL_MYR'].astype(float)

  appendfinal = merge.fillna(0).groupby(['Loan_Acc_'\
  ,'Borrower','Ccy','Type_of_Financing'])[['LAF_ECL_FC'\
  ,'LAF_ECL_MYR','CnC_ECL_FC','CnC_ECL_MYR']].sum().reset_index().drop_duplicates('Loan_Acc_', keep='first')

  #
  
  appendfinal.rename(columns={'Loan_Acc_':'Account'},inplace=True)
  appendfinal['Account'] = appendfinal['Account'].astype(str)

  LDB_prev['Finance(SAP) Number'] = LDB_prev['Finance(SAP) Number'].astype(str)

  LDB_prev.columns = LDB_prev.columns.str.replace("\n", "")

  appendfinal1 = appendfinal.merge(LDB_prev[['Finance(SAP) Number','Facility Currency','Status']].drop_duplicates('Finance(SAP) Number',keep='first').rename(columns={'Finance(SAP) Number':'Account'}),on=['Account'],how='left', suffixes=('_x', '')) #,indicator=True


  #---------------------------------------------Download-------------------------------------------------------------

  #st.write('Sum Total Loans Outstanding (MYR) : RM'+str(sum))
  st.write("")
  st.write(f"Sum ECL LAF (FC) : ${float(sum(appendfinal1['LAF_ECL_FC']))}")
  st.write(f"Sum ECL LAF (MYR) : RM{float(sum(appendfinal1['LAF_ECL_MYR']))}")

  st.write("")
  st.write(f"Sum ECL C&C (FC) : ${float(sum(appendfinal1['CnC_ECL_FC']))}")
  st.write(f"Sum ECL C&C (MYR) : RM{float(sum(appendfinal1['CnC_ECL_MYR']))}")
  
  st.write("")
  st.write("Row Column Checking: ")
  st.write(appendfinal1.shape)

  st.write(appendfinal1)
           
  #st.write("SAP Duplication Checking: ")
  #st.write(appendfinal3['Account'].value_counts())

  st.write("Download file: ")
  st.download_button("Download CSV",
                   appendfinal1.to_csv(index=False),
                   file_name='03. Allowance '+str(year)+"-"+str(month)+'.csv',
                   mime='text/csv')