#%% Se nÃ£o tem o pacote do oracle instalado
#pip install cx_Oracle --upgrade
#pip install config
#%% Imports
import cx_Oracle
import pandas as pd
import numpy as np
import config
#%% string conections 
username = 'FCOSTA'
password = 'mudar@123'
dsn = 'prod-db-dw-001.cfu3pwbfwqo3.us-east-1.rds.amazonaws.com/PRODDBDW'
port = 1521
encoding = 'UTF-8'
#%% setting dates

from datetime import datetime
from dateutil.relativedelta import relativedelta

today = datetime.today().strftime('%Y-%m-%d')
since_aux = datetime.today() + relativedelta(months=-2)
since = since_aux.strftime('%Y-%m-%d')

#%% inicio conexÃ£o
connection = None
try:
    connection = cx_Oracle.connect(username, password, dsn, encoding = encoding)
    # show the version of the Oracle Database
    print(connection.version)
except cx_Oracle.Error as error:
    print(error)
#%% setting SQL Query 
querybeginning = ("""
 SELECT sb.PAY_METHOD_ID
, pm.PAY_METHOD_NAME
, TO_CHAR(sb.DATE_BRL_DAY, 'YYYY-MM-DD') AS ANO_MES
, SUM(sb.EXPIRED_TOTAL) AS EXPIRED_TOTAL
, SUM(sb.CANCELED_TOTAL ) AS CANCELED_TOTAL 
, SUM(sb.CANCELED_AFTER_TRIAL) AS CANCELED_AFTER_TRIAL
FROM ubook.DW_SUBSCRIPTION_SUMMARY sb LEFT JOIN UBOOK.DW_PAYMENT_METHODS pm ON sb.PAY_METHOD_ID = pm.PAY_METHOD_ID
""")
querydate = " WHERE sb.DATE_BRL_DAY BETWEEN to_date('" + since + "','yyyy-mm-dd') AND to_date('" + today +"','yyyy-mm-dd')"
querygroupby = (" GROUP BY sb.PAY_METHOD_ID, pm.PAY_METHOD_NAME, TO_CHAR(sb.DATE_BRL_DAY, 'YYYY-MM-DD') ORDER BY sb.PAY_METHOD_ID, TO_CHAR(DATE_BRL_DAY, 'YYYY-MM-DD')")

query = querybeginning + querydate + querygroupby
#%% setting SQL Query
subscription = pd.read_sql_query(query, connection)
connection.close()
#%% recodificando null para 0
subscription['EXPIRED_TOTAL'] = subscription['EXPIRED_TOTAL'].fillna(0.0)
subscription['CANCELED_TOTAL'] = subscription['CANCELED_TOTAL'].fillna(0.0)
subscription['CANCELED_AFTER_TRIAL'] = subscription['CANCELED_AFTER_TRIAL'].fillna(0.0)
#%% Describe
#tabela_desc = subscription.describe()
#%%função com informação
def valor_zerado(x):
    if x == 0.0:
        return 1
    else:
        return 0
#%% função percentile

def q50(x):
            return x.quantile(0.5)
# 90th Percentile
def q90(x):
            return x.quantile(0.9)    
   
#%%dias com informação EXPIRED
subscription_expired = subscription[subscription['EXPIRED_TOTAL'] > 0] 
ultimo_dia_expired = subscription_expired.groupby('PAY_METHOD_NAME').agg({'ANO_MES':['max']})
#%%Dias com informação CANCELED
subscription_canceled = subscription[subscription['CANCELED_TOTAL'] > 0] 
ultimo_dia_canceled = subscription_canceled.groupby('PAY_METHOD_NAME').agg({'ANO_MES':['max']})
#%%Qtde de dias sem informação CANCELED e Resumo estatístico
subscription['FLAG_CANCELED'] = subscription['CANCELED_TOTAL'].apply(valor_zerado)
dias_sem_canceled = subscription.groupby('PAY_METHOD_NAME').agg({'FLAG_CANCELED':['sum']})
dias_sem_canceled.columns = dias_sem_canceled.columns.droplevel(0)

media_diaria_canceled = subscription.groupby('PAY_METHOD_NAME').agg({'CANCELED_TOTAL':['mean','std', 'min', 'max', q50, q90]})
media_diaria_canceled.columns = media_diaria_canceled.columns.droplevel(0)
#%%teste manipular dados agregados


