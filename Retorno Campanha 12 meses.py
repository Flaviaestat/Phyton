#%% pacotes necessÃ¡rios
#pip install statsmodels
#pip install cx_Oracle --upgrade
#pip install config

#%% import library
import cx_Oracle
import pandas as pd
import config
import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols
#%% string conections 
username = 'FCOSTA'
password = 'mudar@123'
dsn = 'prod-db-dw-001.cfu3pwbfwqo3.us-east-1.rds.amazonaws.com/PRODDBDW'
port = 1521
encoding = 'UTF-8'
#%% inicio conexÃÂ£o
connection = None
try:
    connection = cx_Oracle.connect(username, password, dsn, encoding = encoding)
    # show the version of the Oracle Database
    print(connection.version)
except cx_Oracle.Error as error:
    print(error)
#%% setting SQL Query
query = ("""      
select 
v.ANO
, TO_CHAR(v.BEGIN_DATE, 'YYYY-MM') AS ANOMES
, v.PERIOD_NAME
, v.PAY_METHOD_NAME 
, TO_CHAR(v.BEGIN_DATE, 'MM') AS mes
, v.PAY_METHOD_SOURCE 
, v.CAMPAIGN_TYPE
, NVL(us.BASE_FINAL,0) AS BASE_MA
, v.GASTO
--, v.BASE_M1 
, v.GUV_M1_ACUM
--, v.GUV_M2_ACUM
--, v.GUV_M3_ACUM
--, v.GUV_M4_ACUM
--, v.GUV_M5_ACUM
, v.GUV_M12_ACUM
from ubook.dw_cohort_roi_campaign_type_vw v
left join ubook.dw_payment_methods pm on pm.pay_method_id = v.pay_method_id
--trazendo a user base do mes anterior para uma determinada forma de pagamento (vai ter replicaÃ§Ã£o em tipo de campanha)
LEFT JOIN (select 
dc.pay_method_name
, TO_CHAR(ADD_MONTHS(ra.BEGIN_DATE,1), 'YYYY-MM') AS ANOMESPOSTERIOR
, SUM(distinct base_final) base_final
from ubook.dw_sales_analytics ra
join ubook.dw_payment_methods dc on dc.pay_method_id = ra.pay_method_id
left join ubook.dw_user_base ub on ub.pay_method_id = ra.pay_method_id and trunc(ub.data_ref,'mm') = trunc(ra.begin_date)
where TO_NUMBER(TO_CHAR(begin_date,'YYYY')) >= TO_NUMBER(TO_CHAR(SYSDATE,'YYYY'))-3
and ra.begin_date <= trunc(sysdate)    
group by dc.pay_method_name, TO_CHAR(ADD_MONTHS(ra.BEGIN_DATE,1), 'YYYY-MM')) US
ON US.ANOMESPOSTERIOR = TO_CHAR(v.BEGIN_DATE, 'YYYY-MM') AND US.pay_method_name = v.PAY_METHOD_NAME 
WHERE TO_NUMBER(TO_CHAR(v.begin_date,'YYYY')) >= TO_NUMBER(TO_CHAR(SYSDATE,'YYYY')) - 3
AND v.GUV_M12_ACUM > 0
ORDER BY v.ano desc
""")
#%% setting SQL Query
dataset = pd.read_sql_query(query, connection)
connection.close()
#%% Definindo colunas
cat_excl = ['PERIOD_NAME','PAY_METHOD_NAME', 'ANOMES']
target   = ['GUV_M12_ACUM']
cat_columns = dataset.drop(cat_excl, axis=1).select_dtypes('object').columns
#%% formando a base de modelagem
#binarizando as categÃ³ricas e retirando os ids
dataset_modelo = pd.get_dummies(dataset.drop(cat_excl, axis=1), columns=cat_columns)
#%% coluna de preditores
preditores = dataset_modelo.drop(target, axis = 1).columns
#%% Split do modelo
from sklearn.model_selection import train_test_split

x_train, x_test, y_train, y_test = train_test_split(dataset_modelo[preditores]
                                                    , dataset_modelo[target]
                                                    , random_state=42, test_size=0.20)
#%% normalizando as numÃ©ricas
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
scaler_model = scaler.fit(x_train)
x_train_scaled = scaler_model.transform(x_train)
x_test_scaled = scaler_model.transform(x_test)
#%% modelo linear 
from sklearn.linear_model import HuberRegressor, LinearRegression
from sklearn import metrics
huber = HuberRegressor().fit(x_train_scaled, y_train.values.ravel())
y_predicted = huber.predict(x_test_scaled)
r2 = metrics.r2_score(y_test, y_predicted).round(4)
mabe = metrics.median_absolute_error(y_test, y_predicted).round(4)
#obs: modelo sem o resultado do primeiro mÃªs tem dificuldades para prever (r2 = 0.3863)
print(r2)
print(mabe)
#%%visualizar o predito e realizado
#comparacao = pd.DataFrame(zip(y_test['GUV_M6_ACUM'], y_predicted), columns =['test', 'prediction'])
#comparacao.to_csv('C:/Users/FlaviaCosta/Google Drive/ProjeÃ§Ãµes/comparacao.csv')
#%% Gerar base de aplicação (campanhas que possuem M1)
#%% inicio conexao
connection = None
try:
    connection = cx_Oracle.connect(username, password, dsn, encoding = encoding)
    # show the version of the Oracle Database
    print(connection.version)
except cx_Oracle.Error as error:
    print(error)
#%% setting SQL Query
query = ("""      
select 
v.ANO
, TO_CHAR(v.BEGIN_DATE, 'YYYY-MM') AS ANOMES
, v.PERIOD_NAME
, v.PAY_METHOD_NAME 
, TO_CHAR(v.BEGIN_DATE, 'MM') AS mes
, v.PAY_METHOD_SOURCE 
, v.CAMPAIGN_TYPE
, NVL(us.BASE_FINAL,0) AS BASE_MA
, v.GASTO
--, v.BASE_M1 
, v.GUV_M1_ACUM
--, v.GUV_M2_ACUM
--, v.GUV_M3_ACUM
--, v.GUV_M4_ACUM
--, v.GUV_M5_ACUM
, v.GUV_M12_ACUM
from ubook.dw_cohort_roi_campaign_type_vw v
left join ubook.dw_payment_methods pm on pm.pay_method_id = v.pay_method_id
--trazendo a user base do mes anterior para uma determinada forma de pagamento (vai ter replicaÃ§Ã£o em tipo de campanha)
LEFT JOIN (select 
dc.pay_method_name
, TO_CHAR(ADD_MONTHS(ra.BEGIN_DATE,1), 'YYYY-MM') AS ANOMESPOSTERIOR
, SUM(distinct base_final) base_final
from ubook.dw_sales_analytics ra
join ubook.dw_payment_methods dc on dc.pay_method_id = ra.pay_method_id
left join ubook.dw_user_base ub on ub.pay_method_id = ra.pay_method_id and trunc(ub.data_ref,'mm') = trunc(ra.begin_date)
where TO_NUMBER(TO_CHAR(begin_date,'YYYY')) >= TO_NUMBER(TO_CHAR(SYSDATE,'YYYY'))-3
and ra.begin_date <= trunc(sysdate)    
group by dc.pay_method_name, TO_CHAR(ADD_MONTHS(ra.BEGIN_DATE,1), 'YYYY-MM')) US
ON US.ANOMESPOSTERIOR = TO_CHAR(v.BEGIN_DATE, 'YYYY-MM') AND US.pay_method_name = v.PAY_METHOD_NAME 
WHERE TO_NUMBER(TO_CHAR(v.begin_date,'YYYY')) >= TO_NUMBER(TO_CHAR(SYSDATE,'YYYY')) - 3
AND v.GUV_M1_ACUM > 0
ORDER BY v.ano desc
""")
#%% setting SQL Query
dataset_aplicacao = pd.read_sql_query(query, connection)
connection.close()
#%% aplicar get dummies
#%% formando a base de modelagem
#binarizando as categÃ³ricas e retirando os ids
dataset_aplicacao_tratado = pd.get_dummies(dataset_aplicacao.drop(cat_excl, axis=1), columns=cat_columns)
#%% aplicar scaler
scaler_model = scaler.fit(dataset_aplicacao_tratado[preditores])
x_aplicacao_scaled = scaler_model.transform(dataset_aplicacao_tratado[preditores])
#%% prediction usando o modelo
y_forecast = huber.predict(x_aplicacao_scaled)
#%% unificar previsÃ£o e dados das campanhas
dataset_aplicacao['FORECAST_GUV_M12_ACUM'] = y_forecast
#tratamentos
dataset_aplicacao.loc[dataset_aplicacao['FORECAST_GUV_M12_ACUM'] <= dataset_aplicacao['GUV_M1_ACUM'], 'FORECAST_GUV_M12_ACUM_F'] = dataset_aplicacao['GUV_M1_ACUM'] * 6
dataset_aplicacao.loc[dataset_aplicacao['FORECAST_GUV_M12_ACUM'] > dataset_aplicacao['GUV_M1_ACUM'], 'FORECAST_GUV_M12_ACUM_F'] = dataset_aplicacao['FORECAST_GUV_M12_ACUM']
dataset_aplicacao['PAG_12_MESES'] = dataset_aplicacao['FORECAST_GUV_M12_ACUM_F'] > dataset_aplicacao['GASTO']
dataset_aplicacao.loc[dataset_aplicacao['GUV_M12_ACUM'] > 0, 'FORECAST_GUV_M12_ACUM_F'] = dataset_aplicacao['GUV_M12_ACUM']

#%% ExportaÃ§Ã£o
dataset_aplicacao.to_csv('C:/Users/FlaviaCosta/Google Drive/Projeções/resultados_roi_12m.csv')
