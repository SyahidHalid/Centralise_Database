import os
import sys
#import config
import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

reportingDate = '2025-06-30'

current_time = pd.Timestamp.now()

try:
    #   pyodbc
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=10.32.1.51,1455;'
        'DATABASE=mis_db_prod11072025;'
        'UID=mis_admin;'
        'PWD=Exim1234;'
        'Encrypt=yes;TrustServerCertificate=yes'  # Use if you encounter SSL issues
    )
    cursor = conn.cursor()


    LDB_prev = pd.read_sql_query(
        "SELECT * FROM dbase_account_hist WHERE position_as_at = ?;",
        conn,
        params=(reportingDate,)
    )

except Exception as e:
    print(f"Connect to Database Error: {e}")

    sys.exit(f"Connect to Database Error: {str(e)}")
        
#----------------------------------------------------------------------------------------------------

#process
try:
    location = r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - ITD\\01. URS & Test Script\\FAD - ECL to MIS Data Patch Jun 25\\"

    file = "ECL S1 S2 Jun-2025 working v5 (2).xlsx"
    
    D1 = "ECL"

    LAF = pd.read_excel(location+file, sheet_name=D1, header=1, dtype={'facility_exim_account_num': str})

    LAF1 = LAF.iloc[np.where(~LAF.facility_exim_account_num.isna())]

    LAF1.facility_exim_account_num = LAF1.facility_exim_account_num.astype(str)    
    LAF1.facility_exim_account_num = LAF1.facility_exim_account_num.str.strip()
    LAF1.facility_exim_account_num = LAF1.facility_exim_account_num.str.replace('-','')
    # LAF1.shape
except Exception as e:
    print(f"Upload Excel Error: {e}")

#------------------------------------------------------------------------------------------------

try:
    LAF1.columns = LAF1.columns.str.replace("\n", "_")
    LAF1.columns = LAF1.columns.str.replace(" ", "_")
    LAF1.columns = LAF1.columns.str.replace(".", "_")
    
    # LAF.iloc[np.where(LAF.facility_exim_account_num == 330801137107038976)]

    # LAF1['facility_exim_account_num'] = LAF1['facility_exim_account_num'].apply(lambda x: '{:.0f}'.format(x))

    #LAF1['facility_exim_account_num'] = LAF1['facility_exim_account_num'].astype(int)
    #LAF1['facility_exim_account_num'] = LAF1['facility_exim_account_num'].astype(str)


    LAF2 = LAF1.fillna(0).groupby(['facility_exim_account_num',
                                   #'Finance_(SAP)_Number',
                                   #'Type_of_Financing',
                                   #'Borrower_name',
                                   'Currency',
                                   #'Watchlist_(Yes/No)',
                                   #'Undrawn/BG',
                                   #'MFRS_staging_',
                                   #'MFRS_staging__1',
                                   #'Staging_movement'
                                   ])[['Total_ECL_MYR_(LAF)',
                                         'Total_ECL_MYR_(C&C)']].sum().reset_index()
    
    
    


    #================================================================================================
    
    # Currency = pd.read_sql_query("""Select facility_exim_account_num
    # ,b.param_name as currency
    # from col_facilities_application_master a
    # left outer join param_system_param b on a.facility_ccy_id = b.param_id;""", conn)

    # # Currency['facility_exim_account_num'].dtypes
    # # Currency['facility_exim_account_num'] = Currency['facility_exim_account_num'].astype(float)
    # Currency.columns = Currency.columns.str.replace("\n", "")

    # LAF1_1 = LAF2.merge(Currency.drop_duplicates('facility_exim_account_num',keep='first').rename(columns={'facility_exim_account_num':'facility_exim_account_num'}),on=['facility_exim_account_num'],how='left', suffixes=('_x', ''),indicator=True) #

    #LAF1_1._merge.value_counts()
    #LAF1_1.iloc[np.where(LAF1_1['_merge']=='left_only')]
    #Currency.iloc[np.where(Currency.finance_sap_number==501086)]


    #view
    #sql ="select * from vw_GetLatestCurrencyRate"
    #MRate = pd.read_sql_query(sql, conn)

    # aa = pd.read_sql_query("""SELECT param_name,r.exchange_rate,r.valuedate
    # FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
    # order by valuedate desc;""", conn)
    
    # SQL query with reportingDate filter
    sql = f"""SELECT TOP 17 param_name, r.exchange_rate, r.valuedate
    FROM [param_ccy_exchange_rate] r
    INNER JOIN param_system_param p 
        ON p.param_reference = 'Root>>Currency' 
        AND currency_id = p.param_id
    WHERE r.valuedate <= '{reportingDate}'
    ORDER BY r.valuedate DESC;
    """
    # Read filtered exchange rates from the database
    MRate1 = pd.read_sql_query(sql, conn)
    
    #MRate1 = aa.iloc[np.where(aa.valuedate==reportingDate)]

    df_add = pd.DataFrame([['MYR','1',reportingDate]], columns=['param_name','exchange_rate','valuedate'])

    MRate = pd.concat([MRate1, df_add])

    MRate['exchange_rate'] = MRate['exchange_rate'].astype(float)

    #====================================================================================================================================
    #import pandas as pd
    #import pyodbc
    #import config
    #from datetime import datetime

    #reportingDate = '2024-03-29'
    #date_obj = datetime.strptime(reportingDate, "%Y-%m-%d")
    
    #conn = pyodbc.connect(config.CONNECTION_STRING)
    #cursor = conn.cursor()

    #aa = pd.read_sql_query("""SELECT param_name,r.exchange_rate,r.valuedate
    #  FROM [param_ccy_exchange_rate] r inner join param_system_param p on p.param_reference ='Root>>Currency' and currency_id=p.param_id
    #  order by param_name asc;""", conn)
    
    #import numpy as np
    #aa1 = aa.iloc[np.where(aa.valuedate==reportingDate)]
    #====================================================================================================================================

    #Rate
    
    LAF3 = LAF2.rename(columns={'Currency':'param_name'}).merge(MRate[['param_name','exchange_rate','valuedate']], on='param_name', how='left',indicator=True) #
    
    # LAF3._merge.value_counts()

    LAF3['exchange_rate'].fillna(1,inplace=True)

    LAF3['Total_ECL_FC_(LAF)'] = LAF3['Total_ECL_MYR_(LAF)'].fillna(0)/LAF3['exchange_rate']
    LAF3['Total_ECL_FC_(C&C)'] = LAF3['Total_ECL_MYR_(C&C)'].fillna(0)/LAF3['exchange_rate']

    

    LAF3.rename(columns={'Total_ECL_FC_(LAF)':'acc_credit_loss_laf_ecl',
                         'Total_ECL_MYR_(LAF)':'acc_credit_loss_laf_ecl_myr',
                         'Total_ECL_FC_(C&C)':'acc_credit_loss_cnc_ecl',
                         'Total_ECL_MYR_(C&C)':'acc_credit_loss_cnc_ecl_myr',},inplace=True)

    LAF3 = LAF3[["facility_exim_account_num","acc_credit_loss_laf_ecl","acc_credit_loss_laf_ecl_myr","acc_credit_loss_cnc_ecl","acc_credit_loss_cnc_ecl_myr"]]

    convert_time = str(current_time).replace(":","-")
    LAF3["position_as_at"] = reportingDate

    # 30952 is Impaired
    #LDB_hist = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = ? and acc_status in (30947,30948,30949,30950);", conn, params=(reportingDate,))
    LDB_name = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = ?;", conn, params=(reportingDate,))
   
    LDB_hist_before = pd.read_sql_query("SELECT * FROM dbase_account_hist where position_as_at = ? and acc_status in (30947,30948,30949,30950);", conn, params=(reportingDate,))
   
    LDB_hist = LDB_hist_before.drop('cif_name', axis=1).merge(LDB_name[['finance_sap_number','cif_name']], on='finance_sap_number', how='left')
   
    LDB_hist.acc_credit_loss_laf_ecl = LDB_hist.acc_credit_loss_laf_ecl.astype(float)
    LDB_hist.acc_credit_loss_laf_ecl_myr = LDB_hist.acc_credit_loss_laf_ecl_myr.astype(float)
    LDB_hist.acc_credit_loss_cnc_ecl = LDB_hist.acc_credit_loss_cnc_ecl.astype(float)
    LDB_hist.acc_credit_loss_cnc_ecl_myr = LDB_hist.acc_credit_loss_cnc_ecl_myr.astype(float)

    condition1 = ~LDB_hist.finance_sap_number.isna()
    condition2 = (LDB_hist.acc_credit_loss_laf_ecl > 0) | (LDB_hist.acc_credit_loss_laf_ecl_myr > 0) | (LDB_hist.acc_credit_loss_cnc_ecl > 0) | (LDB_hist.acc_credit_loss_cnc_ecl_myr > 0)

    LDB_hist.facility_exim_account_num = LDB_hist.facility_exim_account_num.astype(str)    
    #LDB_hist.facility_exim_account_num = LDB_hist.facility_exim_account_num.str.strip()
    LDB_hist.facility_exim_account_num = LDB_hist.facility_exim_account_num.str.replace('-','')
    LDB_hist.facility_exim_account_num = LDB_hist.facility_exim_account_num.str.replace(' ','')

    # LDB_hist_before.head(1)
    LDB_hist1 = LDB_hist[['facility_exim_account_num',
                          'cif_name',
                                                   'acc_credit_loss_laf_ecl',
                                                   'acc_credit_loss_laf_ecl_myr',
                                                   'acc_credit_loss_cnc_ecl',
                                                   'acc_credit_loss_cnc_ecl_myr']] #.iloc[np.where(condition1 & condition2)]
    # appendfinal.head(1)
    # LDB_name.shape
    # LDB_hist_before.facility_exim_account_num.value_counts()
    # LAF3.facility_exim_account_num.dtypes
    # LDB_hist1.iloc[np.where(LDB_hist1.facility_exim_account_num=='')]

    # LAF1 #330801137107039000	AEMULUS

    # LDB_hist1.iloc[np.where(LDB_hist1.facility_exim_account_num=="330801137107020901")]
    # LAF1.iloc[np.where(LAF1.facility_exim_account_num=='330801137107020901')]
    # exception_report.iloc[np.where(exception_report.facility_exim_account_num=='330801137107020901')]
    # exception_report._merge.value_counts()

    exception_report = LAF3.merge(LDB_hist1.drop_duplicates('facility_exim_account_num'), on='facility_exim_account_num', how='outer', suffixes=('_Sap','_Mis'),indicator=True)

    # LAF3.shape

    exception_report["diff_LAF_ECL_FC"] = exception_report["acc_credit_loss_laf_ecl_Sap"].fillna(0) - exception_report["acc_credit_loss_laf_ecl_Mis"].fillna(0)

    exception_report["diff_LAF_ECL_MYR"] = exception_report["acc_credit_loss_laf_ecl_myr_Sap"].fillna(0) - exception_report["acc_credit_loss_laf_ecl_myr_Mis"].fillna(0)
    
    exception_report["diff_CnC_ECL_FC"] = exception_report["acc_credit_loss_cnc_ecl_Sap"].fillna(0) - exception_report["acc_credit_loss_cnc_ecl_Mis"].fillna(0)
    
    exception_report["diff_CnC_ECL_MYR"] = exception_report["acc_credit_loss_cnc_ecl_myr_Sap"].fillna(0) - exception_report["acc_credit_loss_cnc_ecl_myr_Mis"].fillna(0)

    exception_report.position_as_at.fillna(reportingDate,inplace=True)
    
    exception_report1 = exception_report[['facility_exim_account_num',
                                          'cif_name',
                                          'position_as_at',
                                          '_merge',
                                          'acc_credit_loss_laf_ecl_Sap',
                                          'acc_credit_loss_laf_ecl_Mis',
                                          'diff_LAF_ECL_FC',
                                          'acc_credit_loss_laf_ecl_myr_Sap',
                                          'acc_credit_loss_laf_ecl_myr_Mis',
                                          'diff_LAF_ECL_MYR',
                                          'acc_credit_loss_cnc_ecl_Sap',
                                          'acc_credit_loss_cnc_ecl_Mis',
                                          'diff_CnC_ECL_FC',
                                          'acc_credit_loss_cnc_ecl_myr_Sap',
                                          'acc_credit_loss_cnc_ecl_myr_Mis',
                                          'diff_CnC_ECL_MYR']]

    # Extract
    writer2 = pd.ExcelWriter(os.path.join(location,"Result_ECL_to_MIS_"+str(convert_time)[:19]+".xlsx"),engine='xlsxwriter')

    LAF3.to_excel(writer2, sheet_name='Result', index = False)

    exception_report1.to_excel(writer2, sheet_name='Exception', index = False)

    writer2.close()


    # LAF3.to_excel(os.path.join(config.FOLDER_CONFIG["FTP_directory"],"Result_ECL_to_MIS_"+str(convert_time)[:19]+".xlsx"),index=False) #"ECL 1024 - MIS v1.xlsx" #documentName

    #df1 =  config.FOLDER_CONFIG["FTP_directory"]+documentName #"ECL 1024 - MIS v1.xlsx" #documentName

    # cursor.execute("DROP TABLE IF EXISTS Exception_ECL_to_MIS")
    # conn.commit()

    # exception_report1._merge = exception_report1._merge.astype(str)
    # exception_report1.fillna(0,inplace=True)
    
    # # Assuming 'combine2' is a DataFrame
    # column_types1 = []
    # for col in exception_report1.columns:
    #     # You can choose to map column types based on data types in the DataFrame, for example:
    #     if exception_report1[col].dtype == 'object':  # String data type
    #         column_types1.append(f"{col} VARCHAR(255)")
    #     elif exception_report1[col].dtype == 'int64':  # Integer data type
    #         column_types1.append(f"{col} INT")
    #     elif exception_report1[col].dtype == 'float64':  # Float data type
    #         column_types1.append(f"{col} FLOAT")
    #     else:
    #         column_types1.append(f"{col} VARCHAR(255)")  # Default type for others

    # #   exception_report1.dtypes
    # create_table_query_result = "CREATE TABLE Exception_ECL_to_MIS (" + ', '.join(column_types1) + ")"
    # cursor.execute(create_table_query_result)

    # for row in exception_report1.iterrows():
    #     sql_result = "INSERT INTO Exception_ECL_to_MIS({}) VALUES ({})".format(','.join(exception_report1.columns), ','.join(['?']*len(exception_report1.columns)))
    #     cursor.execute(sql_result, tuple(row[1]))
    # conn.commit()

except Exception as e:
    print(f"Process Excel Error: {e}")


#--------------------------------------------------------connect ngan database-----------------------------------------------------------------------------------------------------------------------------------------------------

# cntrl + K + C untuk comment kn sume 
# cntrl + K + U untuk comment kn sume 

try:
    # Assuming 'combine2' is a DataFrame
    column_types = []
    for col in LAF3.columns:
        # You can choose to map column types based on data types in the DataFrame, for example:
        if LAF3[col].dtype == 'object':  # String data type
            column_types.append(f"{col} VARCHAR(255)")
        elif LAF3[col].dtype == 'int64':  # Integer data type
            column_types.append(f"{col} INT")
        elif LAF3[col].dtype == 'float64':  # Float data type
            column_types.append(f"{col} FLOAT")
        else:
            column_types.append(f"{col} VARCHAR(255)")  # Default type for others


    # Generate the CREATE TABLE statement
    create_table_query = "CREATE TABLE A_ECL_TO_MIS (" + ', '.join(column_types) + ")"
    # Execute the query
    cursor.execute(create_table_query)

    for row in LAF3.iterrows():
        sql = "INSERT INTO A_ECL_TO_MIS({}) VALUES ({})".format(','.join(LAF3.columns), ','.join(['?']*len(LAF3.columns)))
        cursor.execute(sql, tuple(row[1]))
    conn.commit()

    # LAF3["facility_exim_account_num"].value_counts()
    # LAF3.iloc[np.where(LAF3["facility_exim_account_num"]=="330801137110034000")]

    cursor.execute("""MERGE INTO dbase_account_hist AS target USING A_ECL_TO_MIS AS source
    ON target.facility_exim_account_num = source.facility_exim_account_num
    WHEN MATCHED AND target.position_as_at = ? THEN
        UPDATE SET target.acc_credit_loss_laf_ecl = source.acc_credit_loss_laf_ecl,
                target.acc_credit_loss_laf_ecl_myr = source.acc_credit_loss_laf_ecl_myr,
                target.acc_credit_loss_cnc_ecl = source.acc_credit_loss_cnc_ecl,
                target.acc_credit_loss_cnc_ecl_myr = source.acc_credit_loss_cnc_ecl_myr;
    """, (reportingDate,))
    conn.commit() 

    # #incase manual upload
    # cursor.execute("""MERGE INTO dbase_account_hist AS target 
    # USING A_ECL_TO_MIS AS source
    # ON target.facility_exim_account_num = source.facility_exim_account_num
    # WHEN MATCHED AND target.position_as_at = '2025-05-31' THEN
    #     UPDATE SET target.acc_credit_loss_laf_ecl = source.acc_credit_loss_laf_ecl,
    #             target.acc_credit_loss_laf_ecl_myr = source.acc_credit_loss_laf_ecl_myr,
    #             target.acc_credit_loss_cnc_ecl = source.acc_credit_loss_cnc_ecl,
    #             target.acc_credit_loss_cnc_ecl_myr = source.acc_credit_loss_cnc_ecl_myr;
    # """)
    # conn.commit() 

    cursor.execute("drop table A_ECL_TO_MIS")
    conn.commit() 

    print("Data updated successfully at "+str(current_time))
    conn.close()
except Exception as e:
    print(f"Update Database Error: {e}")
