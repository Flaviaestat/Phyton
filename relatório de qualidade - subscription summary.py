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
#inserir senha banco oracle
password = 
dsn = 'prod-db-dw-001.cfu3pwbfwqo3.us-east-1.rds.amazonaws.com/PRODDBDW'
port = 1521
encoding = 'UTF-8'
#%% setting dates - analisar últimos três meses
from datetime import datetime
from dateutil.relativedelta import relativedelta
today = datetime.today().strftime('%Y-%m-%d')
since_aux = datetime.today() + relativedelta(months=-3)
since = since_aux.strftime('%Y-%m-%d')
#%% inicio conexÃ£o
connection = None
try:
    connection = cx_Oracle.connect(username, password, dsn, encoding = encoding)
    # show the version of the Oracle Database
    print(connection.version)
except cx_Oracle.Error as error:
    print(error)
#%% setting SQL Query - investigação base subscription summary
querybeginning = ("""
 SELECT sb.PAY_METHOD_ID
, pm.PAY_METHOD_NAME
, TO_CHAR(sb.DATE_BRL_DAY, 'YYYY-MM-DD') AS ANO_MES
, SUM(NVL(sb.NEW_TOTAL,0)) AS NEW_TOTAL
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
ultimo_dia_expired.columns = ultimo_dia_expired.columns.droplevel(0)
#%%Dias com informação CANCELED
subscription_canceled = subscription[subscription['CANCELED_TOTAL'] > 0] 
ultimo_dia_canceled = subscription_canceled.groupby('PAY_METHOD_NAME').agg({'ANO_MES':['max']})
ultimo_dia_canceled.columns = ultimo_dia_canceled.columns.droplevel(0)
#%%Dias com informação NEW
subscription_new = subscription[subscription['NEW_TOTAL'] > 0] 
ultimo_dia_new = subscription_new.groupby('PAY_METHOD_NAME').agg({'ANO_MES':['max']})
ultimo_dia_new.columns = ultimo_dia_new.columns.droplevel(0)
#%%Qtde de dias sem informação CANCELED e Resumo estatístico
subscription['FLAG_CANCELED'] = subscription['CANCELED_TOTAL'].apply(valor_zerado)
dias_sem_canceled = subscription.groupby('PAY_METHOD_NAME').agg({'FLAG_CANCELED':['sum']})
dias_sem_canceled.columns = dias_sem_canceled.columns.droplevel(0)
media_diaria_canceled = subscription.groupby('PAY_METHOD_NAME').agg({'CANCELED_TOTAL':['mean','std', 'min', 'max', q50, q90]})
media_diaria_canceled.columns = media_diaria_canceled.columns.droplevel(0)
#%%Qtde de dias sem informação EXPIRED e Resumo estatístico
subscription['FLAG_EXPIRED'] = subscription['EXPIRED_TOTAL'].apply(valor_zerado)
dias_sem_expired = subscription.groupby('PAY_METHOD_NAME').agg({'FLAG_EXPIRED':['sum']})
dias_sem_expired.columns = dias_sem_expired.columns.droplevel(0)
media_diaria_expired = subscription.groupby('PAY_METHOD_NAME').agg({'EXPIRED_TOTAL':['mean','std', 'min', 'max', q50, q90]})
media_diaria_expired.columns = media_diaria_expired.columns.droplevel(0)
#%%Qtde de dias sem informação NEW_TOTAL e Resumo estatístico
subscription['FLAG_NEW'] = subscription['NEW_TOTAL'].apply(valor_zerado)
dias_sem_new = subscription.groupby('PAY_METHOD_NAME').agg({'FLAG_NEW':['sum']})
dias_sem_new.columns = dias_sem_new.columns.droplevel(0)
media_diaria_new = subscription.groupby('PAY_METHOD_NAME').agg({'NEW_TOTAL':['mean','std', 'min', 'max', q50, q90]})
media_diaria_new.columns = media_diaria_new.columns.droplevel(0)
#%%Outliers Canceled
subscription_canceled_final = subscription_canceled[['CANCELED_TOTAL', 'PAY_METHOD_NAME', 'ANO_MES']].set_index('PAY_METHOD_NAME').join(media_diaria_canceled, on = 'PAY_METHOD_NAME', how = 'left' )
subscription_canceled_final['Z'] = (subscription_canceled_final['CANCELED_TOTAL'] - subscription_canceled_final['mean']) / subscription_canceled_final['std']
#estabelecendo z value de 2,5 para detectar outliers
subscription_canceled_outliers = subscription_canceled_final[np.absolute(subscription_canceled_final['Z']) > 2.5]
#%%Outliers Expired
subscription_expired_final = subscription_expired[['EXPIRED_TOTAL', 'PAY_METHOD_NAME', 'ANO_MES']].set_index('PAY_METHOD_NAME').join(media_diaria_expired, on = 'PAY_METHOD_NAME', how = 'left' )
subscription_expired_final['Z'] = (subscription_expired_final['EXPIRED_TOTAL'] - subscription_expired_final['mean']) / subscription_expired_final['std']
#estabelecendo z value de 2,5 para detectar outliers
subscription_expired_outliers = subscription_expired_final[np.absolute(subscription_expired_final['Z']) > 2.5]
#%%Outliers New
subscription_new_final = subscription_new[['NEW_TOTAL', 'PAY_METHOD_NAME', 'ANO_MES']].set_index('PAY_METHOD_NAME').join(media_diaria_new, on = 'PAY_METHOD_NAME', how = 'left' )
subscription_new_final['Z'] = (subscription_new_final['NEW_TOTAL'] - subscription_new_final['mean']) / subscription_new_final['std']
#estabelecendo z value de 2,5 para detectar outliers
subscription_new_outliers = subscription_new_final[np.absolute(subscription_new_final['Z']) > 2.5]
#%% juntar dias sem + ultimo dia e exportar
falta_valores_canceled = dias_sem_canceled.join(ultimo_dia_canceled, on = 'PAY_METHOD_NAME' , how = 'left')
falta_valores_canceled.columns = ['DIAS_SEM_INFO','ULTIMO_DIA']
falta_valores_canceled.to_csv('C:/Users/FlaviaCosta/Google Drive/Testes Phyton/report_canceled.csv')

falta_valores_expired = dias_sem_expired.join(ultimo_dia_expired, on = 'PAY_METHOD_NAME' , how = 'left')
falta_valores_expired.columns = ['DIAS_SEM_INFO','ULTIMO_DIA']
falta_valores_expired.to_csv('C:/Users/FlaviaCosta/Google Drive/Testes Phyton/report_expired.csv')

falta_valores_new = dias_sem_new.join(ultimo_dia_new, on = 'PAY_METHOD_NAME' , how = 'left')
falta_valores_new.columns = ['DIAS_SEM_INFO','ULTIMO_DIA']
falta_valores_new.to_csv('C:/Users/FlaviaCosta/Google Drive/Testes Phyton/report_new.csv')
#%% exportar outliers
subscription_canceled_outliers.to_csv('C:/Users/FlaviaCosta/Google Drive/Testes Phyton/subscription_canceled_outliers.csv')
subscription_expired_outliers.to_csv('C:/Users/FlaviaCosta/Google Drive/Testes Phyton/subscription_expired_outliers.csv')
subscription_new_outliers.to_csv('C:/Users/FlaviaCosta/Google Drive/Testes Phyton/subscription_new_outliers.csv')

