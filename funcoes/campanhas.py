import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import boto3
import io
from io import StringIO
from urllib.parse import quote_plus  # PY2: from urllib import quote_plus
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Table, MetaData
import gspread
from google.oauth2 import service_account
import perfil as pf
from scipy import stats
from datetime import datetime
from dateutil.relativedelta import relativedelta

# base_camp = importa_campanha('data_science/team/Flavia/Volumetria_CRM.csv', ';')

def importa_campanha(KEY, delim):
    REGION = 'us-east-1'
    ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID_PAG') 
    SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY_PAG') 
    BUCKET_NAME = 'datalake-grupoavista'
    s3c = boto3.client(
            's3', 
            region_name = REGION,
            aws_access_key_id = ACCESS_KEY_ID,
            aws_secret_access_key = SECRET_ACCESS_KEY
        )

    obj = s3c.get_object(Bucket= BUCKET_NAME , Key = KEY)
    dfaux = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8', delimiter = delim, chunksize=300000)
    df = pd.concat(dfaux)
    return df


def cria_df_conversao(usuario_athena, dt_ini, dt_fim, chave, path_campanha, arquivo_campanha, delim):
    
    import warnings
    warnings.filterwarnings('ignore')
    import sys
    sys.tracebacklimit=0
    df = importa_campanha(path_campanha + arquivo_campanha, delim)
    ACCESS_KEY_ID_WILL = os.getenv('AWS_ACCESS_KEY_ID_WILL')
    SECRET_ACCESS_KEY_WILL = os.getenv('AWS_SECRET_ACCESS_KEY_WILL')
    STAGING_DIR = 's3://data-athena-query-result-will-prod/' + usuario_athena
    SCHEMA = usuario_athena
    con1 = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"
    con2 = "{schema_name}?s3_staging_dir={s3_staging_dir}"
    conn_str = con1 + con2
    engine_athena = create_engine(conn_str.format(
        aws_access_key_id=quote_plus(ACCESS_KEY_ID_WILL),
        aws_secret_access_key=quote_plus(SECRET_ACCESS_KEY_WILL),
        region_name="sa-east-1",
        schema_name=SCHEMA,
        s3_staging_dir=quote_plus(STAGING_DIR)))
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    1 as converteu,
                    sum(transacoes) as transacoes,
                    sum(spending) as spending
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia >= '"""
    
    query_final = query + dt_ini + "' and dia <= '" + dt_fim + "' group by 1"
    try:
        df_cv = pd.read_sql(query_final, engine_athena)
    except HTMLParseError:
        pass
    
    sys.tracebacklimit=None
    
    df_cv['cpf'] = df_cv['cpf'].astype('string').str.zfill(11)
    
    if chave == 'cpf':
        df['cpf'] = df['cpf'].astype('string').str.zfill(11)
    else:
        pass
        
    df = df.join(df_cv.set_index(chave), how = 'left', on = chave)
    #tratar os nulls da flag converteu, spending e transações
    df['converteu'] = df['converteu'].fillna(0.0)
    return df


def relatorio_campanhas(usuario_athena, dt_ini, dt_fim, chave, path_campanha, arquivo_campanha, delim):
    
    import warnings
    warnings.filterwarnings('ignore')
    import sys
    sys.tracebacklimit=0
    df = importa_campanha(path_campanha + arquivo_campanha, delim)
    ACCESS_KEY_ID_WILL = os.getenv('AWS_ACCESS_KEY_ID_WILL')
    SECRET_ACCESS_KEY_WILL = os.getenv('AWS_SECRET_ACCESS_KEY_WILL')
    STAGING_DIR = 's3://data-athena-query-result-will-prod/' + usuario_athena
    SCHEMA = usuario_athena
    con1 = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"
    con2 = "{schema_name}?s3_staging_dir={s3_staging_dir}"
    conn_str = con1 + con2
    engine_athena = create_engine(conn_str.format(
        aws_access_key_id=quote_plus(ACCESS_KEY_ID_WILL),
        aws_secret_access_key=quote_plus(SECRET_ACCESS_KEY_WILL),
        region_name="sa-east-1",
        schema_name=SCHEMA,
        s3_staging_dir=quote_plus(STAGING_DIR)))
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    1 as converteu,
                    sum(transacoes) as transacoes,
                    sum(spending) as spending
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia >= '"""
    
    query_final = query + dt_ini + "' and dia <= '" + dt_fim + "' group by 1"
    try:
        df_cv = pd.read_sql(query_final, engine_athena)
    except HTMLParseError:
        pass
    
    sys.tracebacklimit=None
    
    df_cv['cpf'] = df_cv['cpf'].astype('string').str.zfill(11)
    
    if chave == 'cpf':
        df['cpf'] = df['cpf'].astype('string').str.zfill(11)
    else:
        pass
        
    df = df.join(df_cv.set_index(chave), how = 'left', on = chave)
    #tratar os nulls da flag converteu, spending e transações
    df['converteu'] = df['converteu'].fillna(0.0)
    df['spending_corr'] = df['spending'].fillna(0.0)
    df['transacoes_corr'] = df['transacoes'].fillna(0.0)
    
    #agregado com media por grupo alvo e controle avg(converteu), avg(spending), avg(frequencia)
    #print dos resultados dos testes de diferença de grupos: conversão, spending, frequencia
      
    agg = df.groupby('grupo').agg({'grupo':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    #lift do resultado das tabelas acima

    print("Lift da conversão: "     + str(round(((agg.iloc[0,1] / agg.iloc[1,1]) -1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['converteu'], df.query('grupo == "controle"')['converteu'])
    print("Estatística t do teste de diferença de proporções: "+str(round(t,2)))
    print("P-valor do teste de diferença de proporções: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de conversão não é significativa")
    else:
        print("A diferença de conversão é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift do spending: "     + str(round(((agg.iloc[0,2] / agg.iloc[1,2]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['spending_corr'], df.query('grupo == "controle"')['spending_corr'])
    print("Estatística t do teste de spending: "+str(round(t,2)))
    print("P-valor do teste de spending: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de spending não é significativa")
    else:
        print("A diferença de spending é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift da frequência: "     + str(round(((agg.iloc[0,4] / agg.iloc[1,4]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['transacoes_corr'], df.query('grupo == "controle"')['transacoes_corr'])
    print("Estatística t do teste de frequencia: "+str(round(t,2)))
    print("P-valor do teste de frequencia: "+str(round(p,6)))
    if p >0.05:
        print("A diferença de frequência não é significativa")
    else:
        print("A diferença de frequência é estatisticamente significativa")
    print("______________________________________________________________")
    
    #criar faixa de frequencia e spending
    def fx_freq(x):
        if x == 1:
            return 'a. 1 compra'
        elif x == 2:
            return 'b. 2 compras'
        elif x == 3:
            return 'c. 3 compras'
        elif x == 4:
            return 'd. 4 compras'
        elif x == 5:
            return 'e. 5 compras'
        elif x > 5:
            return 'f. Mais de 5 compras'   
        else:
            return 'g. Sem compras'
    
    def fx_spend(x):
        if x < 300:
            return 'a. até R$ 300'
        elif x >= 300 and x < 600:
            return 'b. até R$ 600'
        elif x >= 600 and x < 1000:
            return 'c. até R$ 1000'
        elif x >= 1000 and x < 1500:
            return 'd. até R$ 1500'
        elif x >= 1500:
            return 'e. mais de R$ 1500'
        else:
            return 'f. Sem compras'
        
    def fx_lim(x):
        if x < 100:
            return 'a. até R$ 100'
        elif x >= 100 and x < 300:
            return 'b. até R$ 300'
        elif x >= 300 and x < 600:
            return 'c. até R$ 600'
        elif x >= 600 and x < 1000:
            return 'd. até R$ 1000'
        elif x >= 1000 and x < 1500:
            return 'e. até R$ 1500'
        elif x >= 1500:
            return 'f. mais de R$ 1500'
        else:
            return 'g. Sem info'
        
    
    df['fx_frequencia'] = df['transacoes'].apply(fx_freq)
    df['fx_spending'] = df['spending'].apply(fx_spend)
       
    #consulta conversão por dia
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, dia,
                    max(conta_cartao) as conta_cartao,
                    1 as converteu,
                    sum(transacoes) as transacoes,
                    sum(spending) as spending
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia >= '"""
    
    #gráficos conversão por dia - primeiro dia
    
    query_final = query + dt_ini + "' and dia <= '" + dt_fim + "' group by 1,2"
    sys.tracebacklimit=0
    try:
        df_dia = pd.read_sql(query_final, engine_athena)
    except HTMLParseError:
        pass
    sys.tracebacklimit=None
    df_dia['cpf'] = df_dia['cpf'].astype('string').str.zfill(11)
    df_dia = df[[chave, 'grupo']].drop_duplicates().join(df_dia.set_index(chave), how = 'inner', on = chave)
    df_dia = df_dia.groupby(['dia', 'grupo']).agg({'transacoes':['sum']})
    df_dia.columns = df_dia.columns.droplevel(0)
    df_dia.reset_index(level=0, inplace=True)
    df_dia = df_dia.reset_index()
    df_dia = df_dia.sort_values('dia', ascending = True)
    df_dia = df_dia.rename(columns={'sum': 'Transacoes'})
    plt.figure(figsize = (10,8))
    sns.lineplot(x='dia', y='Transacoes',
             hue="grupo", markers=True,
             data=df_dia)
    
    plt.xticks(rotation=45)
    plt.title('Transacoes por dia')
    plt.show()

    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, min(dia) as dia,
                    max(conta_cartao) as conta_cartao,
                    1 as converteu,
                    sum(transacoes) as transacoes,
                    sum(spending) as spending
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia >= '"""
    
    #gráficos conversão por dia
    
    query_final = query + dt_ini + "' and dia <= '" + dt_fim + "' group by 1"
    sys.tracebacklimit=0
    df_dia = pd.read_sql(query_final, engine_athena)
    sys.tracebacklimit=None
    df_dia['cpf'] = df_dia['cpf'].astype('string').str.zfill(11)
    df_dia = df[[chave, 'grupo']].drop_duplicates().join(df_dia.set_index(chave), how = 'inner', on = chave)
    df_dia = df_dia.groupby(['dia', 'grupo']).agg({'transacoes':['sum']})
    df_dia.columns = df_dia.columns.droplevel(0)
    df_dia.reset_index(level=0, inplace=True)
    df_dia = df_dia.reset_index()
    df_dia = df_dia.sort_values('dia', ascending = True)
    df_dia = df_dia.rename(columns={'sum': 'Transacoes'})
    plt.figure(figsize = (10,8))
    sns.lineplot(x='dia', y='Transacoes',
             hue="grupo", markers=True,
             data=df_dia)
    
    plt.xticks(rotation=45)
    plt.title('Transacoes por dia - primeira transação no período')
    plt.show()   
    
    del df_cv
    
    #consulta com características anteriores a campanha (ultimos x dias): recencia, fx_frequencia, fx_spending
    ini = datetime.strptime(dt_ini, '%Y-%m-%d')
    ini_antes = (ini + relativedelta(days=-15)).strftime('%Y-%m-%d')

    query = """
                    With uso_credito_pag as (		
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    min(vl_limite_disp) as vl_limite_disp,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    min(vl_limite_disp) as vl_limite_disp,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                   , base_union as (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    approx_percentile(vl_limite_disp,0.5) as limite_disp_antes,
                    sum(transacoes) as transacoes_antes,
                    sum(spending) as spending_antes
                    from base_union
                   where dia >= '"""
    
    query_final = query + ini_antes + "' and dia < '" + dt_ini + "' group by 1"
    sys.tracebacklimit=0
    df_antes = pd.read_sql(query_final, engine_athena)
    sys.tracebacklimit=None
    df_antes['cpf'] = df_antes['cpf'].astype('string').str.zfill(11)
    df = df.join(df_antes[[chave, 'transacoes_antes', 'spending_antes', 'limite_disp_antes']].set_index(chave), how = 'left', on = chave)
    
    df['fx_frequencia_antes'] = df['transacoes_antes'].apply(fx_freq)
    df['fx_spending_antes'] = df['spending_antes'].apply(fx_spend)
    df['fx_limite'] = df['limite_disp_antes'].apply(fx_lim)
    
    
    print("___________________________________________")
    print("Distribuição de Frequência Antes x Depois")
    print(df['fx_frequencia_antes'].value_counts(normalize = True))
    print(df['fx_frequencia'].value_counts(normalize = True))
    plt.hist([df['fx_frequencia_antes'], df['fx_frequencia']], bins = 20, label=['anterior', 'atual'])  
    plt.legend(loc='upper right')
    plt.title('Frequencia Antes x Depois')
    plt.xticks(rotation=45)
    plt.show()
    
    print("___________________________________________")
    print("Distribuição de Spending Antes x Depois")
    print(df['fx_spending_antes'].value_counts(normalize = True))
    print(df['fx_spending'].value_counts(normalize = True))    
    plt.hist([df['fx_spending_antes'], df['fx_spending']], bins = 20, label=['anterior', 'atual']) 
    plt.legend(loc='upper right')
    plt.title('Antes x Depois')
    plt.xticks(rotation=45)
    plt.show()
    
    
    #conversão por faixa anterior de frequencia e spending
    
    print("___________________________________________")
    print("conversão por faixa anterior de frequencia e spending")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_frequencia_antes').agg({'fx_frequencia_antes':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_spending_antes').agg({'fx_spending_antes':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    print("conversão por limite disponível")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_limite').agg({'fx_limite':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    
    #consulta recência
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    max(dia) as ultima_compra
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia < '"""
    
    query_final = query + dt_ini + "' group by 1"
    sys.tracebacklimit=0
    df_antes = pd.read_sql(query_final, engine_athena)
    sys.tracebacklimit=None
    df_antes['cpf'] = df_antes['cpf'].astype('string').str.zfill(11)
    
    df_antes['dt_ref'] = datetime.strptime(dt_ini, '%Y-%m-%d')
    df_antes['ultima_compra']  = pd.to_datetime(df_antes['ultima_compra'] ,format='%Y-%m-%d')
       
    df_antes['Dif'] = (df_antes['dt_ref'] - df_antes['ultima_compra']).dt.days
    
    def faixas_dias(x):
        if x < 30:
            return 'a. menos 1 mes'
        elif x < 60:
            return 'b. 1 mes'
        elif x < 90:
            return 'c. 2 meses'
        elif x < 120:
            return 'd. 3 meses'
        elif x < 149:
            return 'e. 4 meses'
        elif x < 179:
            return 'f. 5 meses'
        elif x < 209:
            return 'g. 6 meses' 
        elif x < 239:
            return 'h. 7 meses'
        elif x < 269:
            return 'i. 8 meses'
        elif x < 299:
            return 'j. 9 meses' 
        elif x < 329:
            return 'k. 10 meses' 
        elif x < 364:
            return 'l. 11 meses'
        elif x < 7200:
            return 'm. mais de 12 meses'  
        else:
            return 'n. sem transacao'

    df = df.join(df_antes[[chave, 'Dif']].set_index(chave), how = 'left', on = chave)
    
    df['fx_recencia'] = df['Dif'].apply(faixas_dias)
    print("conversão por faixa de recência")
    agg = df[df['grupo'] == 'alvo'].groupby('fx_recencia').agg({'fx_recencia':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
#     print(df.columns)
    #consulta com status de pagamentos
    
    
    query_atr = """
                   select username as cpf,
                    max(nr_days_paste_due) as dias_atraso
                    from (
                    select atr.*, u.username,
                    ROW_NUMBER() OVER(PARTITION by id_customer ORDER BY atr.dt_snapshot_date desc) AS rank_
                    from platform_curated_zone.customer_daily_view atr
                    inner join "processed-zone-database-access-control".user u on (u.id = atr.id_customer)
                    )
                    where rank_ = 1
                    group by 1
                    
                    """

    sys.tracebacklimit=0
    df_atr = pd.read_sql(query_atr, engine_athena)
    sys.tracebacklimit=None   
    df = df.merge(df_atr, how = 'left', on = 'cpf')
    
    df['fx_atraso'] = np.where((df['dias_atraso'] > 5) & (df['dias_atraso'] <= 10), 'B - 5 a 10 dias',
                          np.where((df['dias_atraso'] > 10), 'C - Mais de 10 dias', 'A - Até 5 dias atraso'))
    
    print("Distribuição de atraso nos clientes com conversão: ")
    print(df.query('converteu == 1')['fx_atraso'].value_counts(normalize = True))
    print("Distribuição de atraso nos clientes sem conversão: ")
    print(df.query('converteu == 0')['fx_atraso'].value_counts(normalize = True))  
    
    print("________________Segmentação RFV___________________")
    
    
    
    
    
    print("_____________________")
    print("________________Análises de perfil___________________")
    
    #consulta com perfil sociodemografico
    import perfil as pf
    df_pf = pf.traz_info(usuario_athena, df, 'cpf')
    
    df_pf = df_pf.query('grupo == "alvo"')
    
    pf.graf_catplot(df_pf, 'converteu')
    
    df_pf = df_pf.reset_index()
    
    df_pf['contagem'] = 1
    
    df_pf = df_pf[['pf_genero', 'pf_faixa_idade', 'pf_estado', 'pf_regiao', 'pf_estado_civil', 'pf_profissao', 'pf_escolaridade', 'pf_idade_conta', 'pf_renda_declarada_will', 'pf_top_mcc', 'converteu', 'contagem']]
    
    print("As combinações com maior lift de conversão serão salvas no arquivo report_perfil_total.csv ")
    pf.report_perfil_grupos(df_pf, "converteu", "contagem", 0.03, 50)
    
    #pesquisar biblioteca que roda análise de uplift    
    #indices das vars categoricas de grupo, faixas de recencia, spending e frequencia, e demográficas
    #base balanceada de conversão x não conversão
    #print da base com importância dos campos
    
    
def relatorio_campanhas_sem_grafico(usuario_athena, dt_ini, dt_fim, chave, path_campanha, arquivo_campanha, delim):
    
    import warnings
    warnings.filterwarnings('ignore')
    import sys
    sys.tracebacklimit=0
    df = importa_campanha(path_campanha + arquivo_campanha, delim)
    ACCESS_KEY_ID_WILL = os.getenv('AWS_ACCESS_KEY_ID_WILL')
    SECRET_ACCESS_KEY_WILL = os.getenv('AWS_SECRET_ACCESS_KEY_WILL')
    STAGING_DIR = 's3://data-athena-query-result-will-prod/' + usuario_athena
    SCHEMA = usuario_athena
    con1 = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"
    con2 = "{schema_name}?s3_staging_dir={s3_staging_dir}"
    conn_str = con1 + con2
    engine_athena = create_engine(conn_str.format(
        aws_access_key_id=quote_plus(ACCESS_KEY_ID_WILL),
        aws_secret_access_key=quote_plus(SECRET_ACCESS_KEY_WILL),
        region_name="sa-east-1",
        schema_name=SCHEMA,
        s3_staging_dir=quote_plus(STAGING_DIR)))
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    1 as converteu,
                    sum(transacoes) as transacoes,
                    sum(spending) as spending
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia >= '"""
    
    query_final = query + dt_ini + "' and dia <= '" + dt_fim + "' group by 1"
    try:
        df_cv = pd.read_sql(query_final, engine_athena)
    except HTMLParseError:
        pass
    
    sys.tracebacklimit=None
    
    df_cv['cpf'] = df_cv['cpf'].astype('string').str.zfill(11)
    
    if chave == 'cpf':
        df['cpf'] = df['cpf'].astype('string').str.zfill(11)
    else:
        pass
        
    df = df.join(df_cv.set_index(chave), how = 'left', on = chave)
    #tratar os nulls da flag converteu, spending e transações
    df['converteu'] = df['converteu'].fillna(0.0)
    df['spending_corr'] = df['spending'].fillna(0.0)
    df['transacoes_corr'] = df['transacoes'].fillna(0.0)
    
    #agregado com media por grupo alvo e controle avg(converteu), avg(spending), avg(frequencia)
    #print dos resultados dos testes de diferença de grupos: conversão, spending, frequencia
      
    agg = df.groupby('grupo').agg({'grupo':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    #lift do resultado das tabelas acima

    print("Lift da conversão: "     + str(round(((agg.iloc[0,1] / agg.iloc[1,1]) -1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['converteu'], df.query('grupo == "controle"')['converteu'])
    print("Estatística t do teste de diferença de proporções: "+str(round(t,2)))
    print("P-valor do teste de diferença de proporções: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de conversão não é significativa")
    else:
        print("A diferença de conversão é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift do spending: "     + str(round(((agg.iloc[0,2] / agg.iloc[1,2]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['spending_corr'], df.query('grupo == "controle"')['spending_corr'])
    print("Estatística t do teste de spending: "+str(round(t,2)))
    print("P-valor do teste de spending: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de spending não é significativa")
    else:
        print("A diferença de spending é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift da frequência: "     + str(round(((agg.iloc[0,4] / agg.iloc[1,4]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['transacoes_corr'], df.query('grupo == "controle"')['transacoes_corr'])
    print("Estatística t do teste de frequencia: "+str(round(t,2)))
    print("P-valor do teste de frequencia: "+str(round(p,6)))
    if p >0.05:
        print("A diferença de frequência não é significativa")
    else:
        print("A diferença de frequência é estatisticamente significativa")
    print("______________________________________________________________")
    
    #criar faixa de frequencia e spending
    def fx_freq(x):
        if x == 1:
            return 'a. 1 compra'
        elif x == 2:
            return 'b. 2 compras'
        elif x == 3:
            return 'c. 3 compras'
        elif x == 4:
            return 'd. 4 compras'
        elif x == 5:
            return 'e. 5 compras'
        elif x > 5:
            return 'f. Mais de 5 compras'   
        else:
            return 'g. Sem compras'
    
    def fx_spend(x):
        if x < 300:
            return 'a. até R$ 300'
        elif x >= 300 and x < 600:
            return 'b. até R$ 600'
        elif x >= 600 and x < 1000:
            return 'c. até R$ 1000'
        elif x >= 1000 and x < 1500:
            return 'd. até R$ 1500'
        elif x >= 1500:
            return 'e. mais de R$ 1500'
        else:
            return 'f. Sem compras'
        
    def fx_lim(x):
        if x < 100:
            return 'a. até R$ 100'
        elif x >= 100 and x < 300:
            return 'b. até R$ 300'
        elif x >= 300 and x < 600:
            return 'c. até R$ 600'
        elif x >= 600 and x < 1000:
            return 'd. até R$ 1000'
        elif x >= 1000 and x < 1500:
            return 'e. até R$ 1500'
        elif x >= 1500:
            return 'f. mais de R$ 1500'
        else:
            return 'g. Sem info'
        
    
    df['fx_frequencia'] = df['transacoes'].apply(fx_freq)
    df['fx_spending'] = df['spending'].apply(fx_spend)
       
    
    del df_cv
    
    #consulta com características anteriores a campanha (ultimos x dias): recencia, fx_frequencia, fx_spending
    ini = datetime.strptime(dt_ini, '%Y-%m-%d')
    ini_antes = (ini + relativedelta(days=-15)).strftime('%Y-%m-%d')

    query = """
                    With uso_credito_pag as (		
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    min(vl_limite_disp) as vl_limite_disp,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    min(vl_limite_disp) as vl_limite_disp,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                   , base_union as (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    approx_percentile(vl_limite_disp,0.5) as limite_disp_antes,
                    sum(transacoes) as transacoes_antes,
                    sum(spending) as spending_antes
                    from base_union
                   where dia >= '"""
    
    query_final = query + ini_antes + "' and dia < '" + dt_ini + "' group by 1"
    sys.tracebacklimit=0
    df_antes = pd.read_sql(query_final, engine_athena)
    sys.tracebacklimit=None
    df_antes['cpf'] = df_antes['cpf'].astype('string').str.zfill(11)
    df = df.join(df_antes[[chave, 'transacoes_antes', 'spending_antes', 'limite_disp_antes']].set_index(chave), how = 'left', on = chave)
    
    df['fx_frequencia_antes'] = df['transacoes_antes'].apply(fx_freq)
    df['fx_spending_antes'] = df['spending_antes'].apply(fx_spend)
    df['fx_limite'] = df['limite_disp_antes'].apply(fx_lim)
    
    
    print("___________________________________________")
    print("Distribuição de Frequência Antes x Depois")
    print(df['fx_frequencia_antes'].value_counts(normalize = True))
    print(df['fx_frequencia'].value_counts(normalize = True))
    
    print("___________________________________________")
    print("Distribuição de Spending Antes x Depois")
    print(df['fx_spending_antes'].value_counts(normalize = True))
    print(df['fx_spending'].value_counts(normalize = True))    
    
    
    #conversão por faixa anterior de frequencia e spending
    
    print("___________________________________________")
    print("conversão por faixa anterior de frequencia e spending")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_frequencia_antes').agg({'fx_frequencia_antes':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_spending_antes').agg({'fx_spending_antes':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    print("conversão por limite disponível")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_limite').agg({'fx_limite':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    
    #consulta recência
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    max(dia) as ultima_compra
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia < '"""
    
    query_final = query + dt_ini + "' group by 1"
    sys.tracebacklimit=0
    df_antes = pd.read_sql(query_final, engine_athena)
    sys.tracebacklimit=None
    df_antes['cpf'] = df_antes['cpf'].astype('string').str.zfill(11)
    
    df_antes['dt_ref'] = datetime.strptime(dt_ini, '%Y-%m-%d')
    df_antes['ultima_compra']  = pd.to_datetime(df_antes['ultima_compra'] ,format='%Y-%m-%d')
       
    df_antes['Dif'] = (df_antes['dt_ref'] - df_antes['ultima_compra']).dt.days
    
    def faixas_dias(x):
        if x < 30:
            return 'a. menos 1 mes'
        elif x < 60:
            return 'b. 1 mes'
        elif x < 90:
            return 'c. 2 meses'
        elif x < 120:
            return 'd. 3 meses'
        elif x < 149:
            return 'e. 4 meses'
        elif x < 179:
            return 'f. 5 meses'
        elif x < 209:
            return 'g. 6 meses' 
        elif x < 239:
            return 'h. 7 meses'
        elif x < 269:
            return 'i. 8 meses'
        elif x < 299:
            return 'j. 9 meses' 
        elif x < 329:
            return 'k. 10 meses' 
        elif x < 364:
            return 'l. 11 meses'
        elif x < 7200:
            return 'm. mais de 12 meses'  
        else:
            return 'n. sem transacao'

    df = df.join(df_antes[[chave, 'Dif']].set_index(chave), how = 'left', on = chave)
    
    df['fx_recencia'] = df['Dif'].apply(faixas_dias)
    print("conversão por faixa de recência")
    agg = df[df['grupo'] == 'alvo'].groupby('fx_recencia').agg({'fx_recencia':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
#     print(df.columns)
    #consulta com status de pagamentos
    
    
    query_atr = """
                   select username as cpf,
                    max(nr_days_paste_due) as dias_atraso
                    from (
                    select atr.*, u.username,
                    ROW_NUMBER() OVER(PARTITION by id_customer ORDER BY atr.dt_snapshot_date desc) AS rank_
                    from platform_curated_zone.customer_daily_view atr
                    inner join "processed-zone-database-access-control".user u on (u.id = atr.id_customer)
                    )
                    where rank_ = 1
                    group by 1
                    
                    """

    sys.tracebacklimit=0
    df_atr = pd.read_sql(query_atr, engine_athena)
    sys.tracebacklimit=None   
    df = df.merge(df_atr, how = 'left', on = 'cpf')
    
    df['fx_atraso'] = np.where((df['dias_atraso'] > 5) & (df['dias_atraso'] <= 10), 'B - 5 a 10 dias',
                          np.where((df['dias_atraso'] > 10), 'C - Mais de 10 dias', 'A - Até 5 dias atraso'))
    
    print("Distribuição de atraso nos clientes com conversão: ")
    print(df.query('converteu == 1')['fx_atraso'].value_counts(normalize = True))
    print("Distribuição de atraso nos clientes sem conversão: ")
    print(df.query('converteu == 0')['fx_atraso'].value_counts(normalize = True))  
    
    
    print("_____________________")
    print("________________Análises de perfil___________________")
    
    #consulta com perfil sociodemografico
    import perfil as pf
    df_pf = pf.traz_info(usuario_athena, df, 'cpf')
    
    df_pf = df_pf.query('grupo == "alvo"')
    
    pf.graf_catplot(df_pf, 'converteu')
    
    df_pf = df_pf.reset_index()
    
    df_pf['contagem'] = 1
    
    df_pf = df_pf[['pf_genero', 'pf_faixa_idade', 'pf_estado', 'pf_regiao', 'pf_estado_civil', 'pf_profissao', 'pf_escolaridade', 'pf_idade_conta', 'pf_renda_declarada_will', 'pf_top_mcc', 'converteu', 'contagem']]
    
    print("As combinações com maior lift de conversão serão salvas no arquivo report_perfil_total.csv ")
    pf.report_perfil_grupos(df_pf, "converteu", "contagem", 0.03, 50)
    
    #pesquisar biblioteca que roda análise de uplift    
    #indices das vars categoricas de grupo, faixas de recencia, spending e frequencia, e demográficas
    #base balanceada de conversão x não conversão
    #print da base com importância dos campos




