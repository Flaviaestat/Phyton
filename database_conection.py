#%% Se não tem o pacote do oracle instalado
#pip install cx_Oracle --upgrade
#pip install config
#%% Imports
import cx_Oracle
import pandas as pd
import config
#%% string conections 
username = 'FCOSTA'
password = 'mudar@123'
dsn = 'prod-db-dw-001.cfu3pwbfwqo3.us-east-1.rds.amazonaws.com/PRODDBDW'
port = 1521
encoding = 'UTF-8'
#%% inicio conexão
connection = None
try:
    connection = cx_Oracle.connect(username, password, dsn, encoding = encoding)

    # show the version of the Oracle Database
    print(connection.version)
except cx_Oracle.Error as error:
    print(error)
#%% setting SQL Query
  
query = ("""
         
SELECT TO_CHAR(ANO_MES,'YYYY') ANO
, SUM(AUDIENCIA_TOTAL_CALC) AUDIENCIA_TOTAL_CALC
, SUM(AUDIENCIA_UBOOK_CALC) AUDIENCIA_UBOOK_CALC
, SUM(AUDIENCIA_PROVIDER_CALC) AUDIENCIA_PROVIDER_CALC 
, ROUND(SUM(AUDIENCIA_UBOOK_CALC)/SUM( AUDIENCIA_TOTAL_CALC),4) PCT_UBOOK
, ROUND(SUM(AUDIENCIA_PROVIDER_CALC)/SUM( AUDIENCIA_TOTAL_CALC),4) PCT_PROVIDER
, MAX(ANO_MES)     
FROM(
SELECT ANO_MES, CATALOG_ITEM_ID,CONTRACT_ID, T.TYPE, ITEM_MASTER_TYPE, AUDIENCIA_TOTAL_CALC, AUDIENCIA_UBOOK_CALC, AUDIENCIA_PROVIDER_CALC 
FROM UBOOK.DW_PUBLISHER_AUDIENCE_TMP T
LEFT JOIN UBOOK.UBK_CATALOG_ITEM CI ON CI.ID = T.CATALOG_ITEM_ID
WHERE NOT EXISTS (SELECT 'X' FROM UBOOK.DW_PUBLISHER_BILLING B WHERE B.CATALOG_ITEM_ID = T.CATALOG_ITEM_ID AND B.ANO_MES = T.ANO_MES )
UNION ALL 
SELECT ANO_MES, CATALOG_ITEM_ID,CONTRACT_ID, TYPE, ITEM_MASTER_TYPE, AUDIENCIA_TOTAL_CALC, AUDIENCIA_UBOOK_CALC, AUDIENCIA_PROVIDER_CALC 
FROM UBOOK.DW_PUBLISHER_BILLING ) TMP
WHERE TO_NUMBER(TO_CHAR(ANO_MES,'MM')) <= TO_NUMBER(TO_CHAR(SYSDATE,'MM'))-1
GROUP BY TO_CHAR(ANO_MES,'YYYY')
ORDER BY ANO

""")

#%% setting SQL Query
base = pd.read_sql_query(query, connection)
connection.close()
#%% working with the dataset