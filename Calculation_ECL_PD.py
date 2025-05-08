import pandas as pd
import numpy as np
import pyodbc
import datetime as dt

pd.set_option("display.max_columns", None) 
pd.set_option("display.max_colwidth", 1000) #huruf dlm column
pd.set_option("display.max_rows", 100)
pd.set_option("display.precision", 2) #2 titik perpuluhan

#Upload PD FL & PD LIFETIME to DB

# #UAT
# conn = pyodbc.connect(
#     'DRIVER={ODBC Driver 17 for SQL Server};'
#     'SERVER=10.32.1.51,1455;'
#     'DATABASE=mis_db_prod_backup_2024_04_02;'
#     'UID=mis_admin;'
#     'PWD=Exim1234;'
#     'Encrypt=yes;TrustServerCertificate=yes;'
# )

# cursor = conn.cursor()

# #PROD
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.20.1.19,1455;'
    'DATABASE=mis_db_prod;'
    'UID=mis_admin;'
    'PWD=Exim1234;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)

cursor = conn.cursor()

PD = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - FAD\\00. Loan Database\\Data Source\\202503\\Risk\\03. ECL Computation Client Template March 2025 (Regular).xlsm", sheet_name='Lifetime PD', header=55, usecols="B:FZ") #

# PD.columns = PD.columns.astype(str)

# PD.head(1)

# PD.dtypes

PD.PD = PD.PD.str.upper()

Pivoted_PD = PD.melt(id_vars="PD",value_vars=[1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13	,14	,15	,16	,17	,18	,19	,20	,21	,22	,23	,24	,25	,26	,27	,28	,29	,30	,31	,32	,33	,34	,35	,36	,37	,38	,39	,40	,41	,42	,43	,44	,45	,46	,47	,48	,49	,50	,51	,52	,53	,54	,55	,56	,57	,58	,59	,60	,61	,62	,63	,64	,65	,66	,67	,68	,69	,70	,71	,72	,73	,74	,75	,76	,77	,78	,79	,80	,81	,82	,83	,84	,85	,86	,87	,88	,89	,90	,91	,92	,93	,94	,95	,96	,97	,98	,99	,100	,101	,102	,103	,104	,105	,106	,107	,108	,109	,110 ,111	,112	,113	,114	,115	,116	,117	,118	,119	,120	,121	,122	,123	,124	,125	,126	,127	,128	,129	,130	,131	,132	,133	,134	,135	,136	,137	,138	,139	,140	,141	,142	,143	,144	,145	,146	,147	,148	,149	,150	,151	,152	,153	,154	,155	,156	,157	,158	,159	,160	,161	,162	,163	,164	,165	,166	,167	,168	,169	,170	,171	,172	,173	,174	,175	,176	,177	,178	,179,  180],var_name="Year",value_name="PD_PERCENTAGE")

column_types = []
for col in Pivoted_PD.columns:
    # You can choose to map column types based on data types in the DataFrame, for example:
    if Pivoted_PD[col].dtype == 'object':  # String data type
        column_types.append(f"{col} VARCHAR(255)")
    elif Pivoted_PD[col].dtype == 'int64':  # Integer data type
        column_types.append(f"{col} INT")
    elif Pivoted_PD[col].dtype == 'float64':  # Float data type
        column_types.append(f"{col} FLOAT")
    else:
        column_types.append(f"{col} VARCHAR(255)")  # Default type for others

# Generate the CREATE TABLE statement
create_table_query = "CREATE TABLE ECL_PD_LIFETIME (" + ', '.join(column_types) + ")"
# Execute the query
cursor.execute(create_table_query)

# for row in PD.iterrows():
#     sql = "INSERT INTO ECL_PD_LIFETIME({}) VALUES ({})".format(
#         ','.join(Pivoted_PD.columns), 
#         ','.join(['?']*len(Pivoted_PD.columns)))
#     cursor.execute(sql, tuple(row[1]))

for _, row in Pivoted_PD.iterrows():
    sql = "INSERT INTO ECL_PD_LIFETIME({}) VALUES ({})".format(
        ','.join([f'[{col}]' for col in Pivoted_PD.columns]),
        ','.join(['?'] * len(Pivoted_PD.columns))
    )
    cursor.execute(sql, tuple(row))

conn.commit()

FL_PD = pd.read_excel(r"C:\\Users\\syahidhalid\\Syahid_PC\\Analytics - FAD\\00. Loan Database\\Data Source\\202503\\Risk\\03. ECL Computation Client Template March 2025 (Regular).xlsm", sheet_name='FL PD', header=59, usecols="B:FZ")

FL_PD.PD = FL_PD.PD.str.upper()

Pivoted_FL_PD = FL_PD.melt(id_vars="PD",value_vars=[1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13	,14	,15	,16	,17	,18	,19	,20	,21	,22	,23	,24	,25	,26	,27	,28	,29	,30	,31	,32	,33	,34	,35	,36	,37	,38	,39	,40	,41	,42	,43	,44	,45	,46	,47	,48	,49	,50	,51	,52	,53	,54	,55	,56	,57	,58	,59	,60	,61	,62	,63	,64	,65	,66	,67	,68	,69	,70	,71	,72	,73	,74	,75	,76	,77	,78	,79	,80	,81	,82	,83	,84	,85	,86	,87	,88	,89	,90	,91	,92	,93	,94	,95	,96	,97	,98	,99	,100	,101	,102	,103	,104	,105	,106	,107	,108	,109	,110 ,111	,112	,113	,114	,115	,116	,117	,118	,119	,120	,121	,122	,123	,124	,125	,126	,127	,128	,129	,130	,131	,132	,133	,134	,135	,136	,137	,138	,139	,140	,141	,142	,143	,144	,145	,146	,147	,148	,149	,150	,151	,152	,153	,154	,155	,156	,157	,158	,159	,160	,161	,162	,163	,164	,165	,166	,167	,168	,169	,170	,171	,172	,173	,174	,175	,176	,177	,178	,179,180],var_name="Year",value_name="FL_PD_PERCENTAGE")

column_types = []
for col in Pivoted_FL_PD.columns:
    # You can choose to map column types based on data types in the DataFrame, for example:
    if Pivoted_FL_PD[col].dtype == 'object':  # String data type
        column_types.append(f"{col} VARCHAR(255)")
    elif Pivoted_FL_PD[col].dtype == 'int64':  # Integer data type
        column_types.append(f"{col} INT")
    elif Pivoted_FL_PD[col].dtype == 'float64':  # Float data type
        column_types.append(f"{col} FLOAT")
    else:
        column_types.append(f"{col} VARCHAR(255)")  # Default type for others

# Generate the CREATE TABLE statement
create_table_query = "CREATE TABLE ECL_PD_FORWARD (" + ', '.join(column_types) + ")"
# Execute the query
cursor.execute(create_table_query)

# for row in PD.iterrows():
#     sql = "INSERT INTO ECL_PD_LIFETIME({}) VALUES ({})".format(
#         ','.join(Pivoted_PD.columns), 
#         ','.join(['?']*len(Pivoted_PD.columns)))
#     cursor.execute(sql, tuple(row[1]))

for _, row in Pivoted_FL_PD.iterrows():
    sql = "INSERT INTO ECL_PD_FORWARD({}) VALUES ({})".format(
        ','.join([f'[{col}]' for col in Pivoted_FL_PD.columns]),
        ','.join(['?'] * len(Pivoted_FL_PD.columns))
    )
    cursor.execute(sql, tuple(row))

conn.commit()




#A = pd.read_sql_query("SELECT * FROM view_ecl_report_data", conn)