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


def traz_info(usuario, df, campocpf):
#     import sys
#     sys.tracebacklimit=0
    ACCESS_KEY_ID_WILL = os.getenv('AWS_ACCESS_KEY_ID_WILL')
    SECRET_ACCESS_KEY_WILL = os.getenv('AWS_SECRET_ACCESS_KEY_WILL')
    STAGING_DIR = 's3://data-athena-query-result-will-prod/' + usuario
    SCHEMA = usuario
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
               with uso_credito_pag_meses as (		
                        select		
                        nr_cpf, vl_real, id_transaction, ds_nome_estabelecimento, ds_mcc, dt_autorizacao
                        from	platform_curated_zone.authorization_pag	
                        where	is_saque = false	
                            and	is_cancelada = false		
                            and	is_recarga = false		
                            and	is_pag_limite_utilizado = 'C'
                            and ds_customer = 'pag'	
                        )		
                        , uso_credito_will_meses as (		
                        select		
                        nr_cpf, vl_real, id_transaction, ds_nome_estabelecimento, ds_mcc, dt_autorizacao
                        from	platform_curated_zone.authorization_will	
                        where   ds_transacao = 'credito'		
                        and	ds_status_compra = 'aprovada'	
                        and	cd_retorno = '00'
                        and ds_mcc != 'TRANSACOES WEBSERVICE'
                        )		
                        --append das duas		
                        , uso_credito_geral_meses as (		
                        select  *		
                        from  uso_credito_pag_meses		
                        union all		
                        select  *		
                        from  uso_credito_will_meses		
                        )
                        , ranking_mcc as (                       
						select * from (
                        select *, rank() over (partition by nr_cpf order by transacoes_hist desc) as r
                        from (
                        select 
                        nr_cpf, upper(ds_mcc) as ds_mcc,
						count(distinct id_transaction) as transacoes_hist		
                        from uso_credito_geral_meses
                        group by 1,2
                        ) ) having r = 1
                        )
                        , base_transacional as (
                        select m.nr_cpf as cpf,
						max(r.ds_mcc) as pf_top_mcc,
						min(date_diff('day', cast(dt_autorizacao as timestamp), current_timestamp)) as recencia,
                        sum(case when date_diff('day', cast(dt_autorizacao as timestamp), current_timestamp) <= 90 then vl_real else 0 end) as spending90dias,
                        count(distinct case when date_diff('day', cast(dt_autorizacao as timestamp), current_timestamp) <= 90 then id_transaction end) as transacoes90dias,
                        sum(case when date_diff('day', cast(dt_autorizacao as timestamp), current_timestamp) <= 360 then vl_real else 0 end) as spending_ultimo_ano,
                        count(distinct case when date_diff('day', cast(dt_autorizacao as timestamp), current_timestamp) <= 360 then id_transaction end) as transacoes_ultimo_ano,
                        sum(vl_real) as vl_real_hist,
                        count(distinct id_transaction) as transacoes_hist		
                        from uso_credito_geral_meses m
                        left join ranking_mcc r on (m.nr_cpf = r.nr_cpf)
                        group by 1
                )
                select distinct * from (
                select 
                p.cd_cpf as chave_or,
                p.ds_origin,
                p.dt_birth as pf_dt_nascimento,
                rank() over (partition by p.cd_cpf order by dt_created_proposal desc) as base_perfil,
                case 
                when p.ds_origin = 'will' and g.ds_gender = 'F' then 'FEMALE'
                when p.ds_origin = 'will' and g.ds_gender = 'M' then 'MALE'
                when p.ds_origin <> 'will' and p.ds_gender = 'F' then 'FEMALE'
                when p.ds_origin <> 'will' and p.ds_gender = 'M' then 'MALE'
                when p.ds_origin = 'will' and g.ds_gender = 'FEMALE' then 'FEMALE'
                when p.ds_origin = 'will' and g.ds_gender = 'MALE' then 'MALE'
                when p.ds_origin <> 'will' and p.ds_gender = 'FEMALE' then 'FEMALE'
                when p.ds_origin <> 'will' and p.ds_gender = 'MALE' then 'MALE'              
                else '' end as pf_genero, 
                case 
                when ds_schooling = 'INCOMPLETE_ELEMENTARY' then 'a - Ensino fundamental incompleto' 
                when ds_schooling = 'COMPLETE_ELEMENTARY' then 'b - Ensino fundamental completo'
                when ds_schooling = 'INCOMPLETE_HIGH_SCHOOL' then 'c - Ensino médio incompleto'
                when ds_schooling = 'COMPLETE_HIGH_SCHOOL' then 'd - Ensino médio completo'
                when ds_schooling = 'INCOMPLETE_HIGHER_EDUCATION' then 'e - Ensino superior incompleto'
                when ds_schooling = 'COMPLETE_HIGHER_EDUCATION' then 'f - Ensino superior completo'
                when ds_schooling = 'INCOMPLETE_POST_GRADUATE' then 'g - Pós graduação incompleta'
                when ds_schooling = 'COMPLETE_POST_GRADUATE' then 'h - Pós graduação completa'
                when ds_schooling = 'INCOMPLETE_MASTER' then 'i - Mestrado incompleto'
                when ds_schooling = 'COMPLETE_MASTER' then 'j - Mestrado completo'
                when ds_schooling = 'INCOMPLETE_DOCTORATE' then 'k - Doutorado incompleto'
                when ds_schooling = 'COMPLETE_DOCTORATE' then 'l - Doutorado completo'
                when ds_schooling = 'OTHER' then 'm - Outros'
                end as pf_escolaridade,
                case
                when  pro_nr_rendadeclarada <= 1000 then 'A - ate R$ 1000'
                when  pro_nr_rendadeclarada <= 2000 then 'B - ate R$ 2000'
                when  pro_nr_rendadeclarada <= 3000 then 'C - ate R$ 3000'
                when  pro_nr_rendadeclarada <= 4000 then 'D - ate R$ 4000'
                when  pro_nr_rendadeclarada <= 5000 then 'E - ate R$ 5000'
                when  pro_nr_rendadeclarada > 5000 then 'F - mais de R$ 5000'
                end as pf_renda_declarada_will,
                replace(upper(ds_occupation), '(A)', '') as pf_profissao,
                case 
                when ds_marital_status = 'SINGLE' then 'Solteiro'
                when ds_marital_status IN ('DIVORCED','SEPARATE') then 'Divorciado'
                when ds_marital_status = 'MARRIED' then 'Casado'
                when ds_marital_status = 'WIDOWER' then 'Viuvo'
                when ds_marital_status = 'OTHERS' then 'Outros'
                else 'outros' end pf_estado_civil,
                p.ds_state_abbreviation as pf_estado,               
                case WHEN p.ds_state_abbreviation IN('DF','GO','MS','MT') THEN 'CENTRO-OESTE'
                    WHEN p.ds_state_abbreviation IN('AL','BA','CE','MA','PB','PE','PI','RN','SE') THEN 'NORDESTE'
                    WHEN p.ds_state_abbreviation IN('AC','AM','AP','PA','RO','RR','TO') THEN 'NORTE'
                    WHEN p.ds_state_abbreviation IN('ES','MG','RJ','SP') THEN 'SUDESTE'
                    WHEN p.ds_state_abbreviation IN('PR','RS','SC') THEN 'SUL'
                    ELSE 'Z-OUTROS' END as pf_regiao,
                case
                when round((extract(day from current_date - cast(p.dt_created_user as date))/30.5/12),0) = 0 then 'a - ate 1 ano de conta'
                when round((extract(day from current_date - cast(p.dt_created_user as date))/30.5/12),0) = 1 then 'b - 1-2 anos de conta'
                when round((extract(day from current_date - cast(p.dt_created_user as date))/30.5/12),0) = 2 then 'c - 2-3 anos de conta'
                when round((extract(day from current_date - cast(p.dt_created_user as date))/30.5/12),0) = 3 then 'd - 3-4 anos de conta'
                when round((extract(day from current_date - cast(p.dt_created_user as date))/30.5/12),0) = 4 then 'e - 4-5 anos de conta'
                when round((extract(day from current_date - cast(p.dt_created_user as date))/30.5/12),0) = 5 then 'f - 5-6 anos de conta'
                else cast(round((extract(day from current_date - cast(p.dt_created_user as date))/30.5/12),0) as varchar) end as pf_idade_conta,
                case
                when round((extract(day from current_date - cast(p.dt_birth as date))/30.5/12 / 10),0) = 2 then 'a - ate 24 anos'
                when round((extract(day from current_date - cast(p.dt_birth as date))/30.5/12 / 10),0) = 3 then 'b - 25-34 anos'
                when round((extract(day from current_date - cast(p.dt_birth as date))/30.5/12 / 10),0) = 4 then 'c - 35-44 anos'
                when round((extract(day from current_date - cast(p.dt_birth as date))/30.5/12 / 10),0) = 5 then 'd - 45-54 anos'
                when round((extract(day from current_date - cast(p.dt_birth as date))/30.5/12 / 10),0) = 6 then 'e - 55-64 anos'
                when round((extract(day from current_date - cast(p.dt_birth as date))/30.5/12 / 10),0) = 7 then 'f - 65-74 anos'
                when round((extract(day from current_date - cast(p.dt_birth as date))/30.5/12 / 10),0) = 8 then 'g - 75-84 anos'
                else 'h - idade NI' end as pf_faixa_idade,                
                p.nm_locality as pf_cidade,
                t.pf_top_mcc,
                p.nr_zip_code as pf_cep,
                t.recencia as pf_recencia,
                round(t.recencia / 30, 0) as pf_fx_recencia_mes,
                t.vl_real_hist,
                t.transacoes_hist,
                t.spending90dias,
                t.transacoes90dias,
                t.spending_ultimo_ano,
                t.transacoes_ultimo_ano
                from growth_curated_zone.proposal_general p 
                left join platform_curated_zone.gender_will g on (g.cd_cpf = p.cd_cpf)
                inner join growth_curated_zone.clientes c on (g.cd_cpf = c.cpf)
                left join base_transacional t on (t.cpf = p.cd_cpf)
                left join growth_curated_zone.proposal cr on (p.cd_cpf = cr.pro_nr_cpf)  
                ) where base_perfil = 1
    """
    
    df_pf = pd.read_sql(query, engine_athena)
    df_pf['chave'] = df_pf['chave_or'].astype('string').str.zfill(11)
    df['chave'] = df[campocpf].astype('string').str.zfill(11)
    df = df.join(df_pf.set_index('chave'), how = 'left', on = 'chave')
       
#     sys.tracebacklimit=None
    return df

#campos que as analises de perfil irão trabalhar
cps = ['pf_genero', 'pf_faixa_idade', 'pf_estado', 'pf_regiao', 'pf_estado_civil', 'pf_profissao', 'pf_escolaridade', 'pf_idade_conta', 'pf_renda_declarada_will', 'pf_top_mcc']

cps_graph = ['pf_genero', 'pf_faixa_idade', 'pf_estado', 'pf_regiao', 'pf_estado_civil', 'pf_escolaridade', 'pf_idade_conta', 'pf_renda_declarada_will']



#df já deve ter trazido os campos pela função traz_info
def agg_perfil_geral(df):
    campos_perfil = cps
    chave = ['chave']
    chave = campos_perfil + chave
    agg = df[chave].groupby(campos_perfil, as_index=False).agg({'chave':['count']})
    return agg

#df já deve ter trazido os campos pela função traz_info
def agg_perfil_grupos(df, campogrupo):
    campos_perfil = cps
    chave = ['chave']
    var_grupo = [campogrupo]
    chave = campos_perfil + chave + var_grupo
    campo_agg = campos_perfil + var_grupo
    agg = df[chave].groupby(campo_agg, as_index=False).agg({'chave':['count']})
    return agg


def grafico_perfil(df):
    campos_perfil = cps
    for i in campos_perfil:
        chave = ['chave']
        agg = df.groupby(i).agg({'chave':['count']})
        agg.columns = agg.columns.droplevel(0)
        agg.reset_index(level=0, inplace=True)
        agg = agg.sort_values('count', ascending = False)
        agg = agg.rename(columns={'count': 'Clientes'})
        x = sns.histplot(
            agg,
            y=i,
            weights='Clientes',
            multiple='stack',
            edgecolor='white',
            stat="probability",
              shrink=0.8).set(title=i)
        plt.show()
        print(df[i].value_counts(normalize = True))
        print("_______________________________________")

#criar gráficos
def grafico_perfil_grupos(df, campogrupo):
    campos_perfil = cps
    for i in campos_perfil:
        chave = ['chave']
        agg = df.groupby([i, campogrupo]).agg({'chave':['count']})
        agg.columns = agg.columns.droplevel(0)
        agg.reset_index(level=0, inplace=True)
        agg = agg.sort_values('count', ascending = False)
        agg = agg.rename(columns={'count': 'Clientes'})
        x = sns.histplot(
            agg,
            y=i,
            weights='Clientes',
            hue=campogrupo,
            multiple='stack',
            edgecolor='white',
            stat="probability",
              shrink=0.8).set(title = i)
        plt.show()
        
        print(pd.crosstab(df[i], df[campogrupo], normalize='columns', margins=True).sort_values(by='All', 
                                                                                                ascending=False))
        print("_______________________________________")
        
def graf_catplot(df, campogrupo):
    campos_perfil = cps_graph
    for i in campos_perfil:
        chave = ['chave']
        x = sns.catplot(y = i, hue = campogrupo, kind = 'count', data = df).set(title = i)
        plt.show()
        print(pd.crosstab(df[i], df[campogrupo], normalize='columns', margins=True).sort_values(by='All', ascending=False))
        print("_______________________________________")
        
        
def report_perfil_grupos(df, coluna_grupo, coluna_contagem, dif_var, n_var):
    from collections import OrderedDict
    casos_signif_final = pd.DataFrame([], columns=['segmento', 'n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca','diferenca_abs'])
    # 'segmento', 'n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca','diferenca_abs'
    casos_signif_final[coluna_grupo] = []
    colunas_perfil = df.drop(coluna_contagem, axis = 1).drop(coluna_grupo, axis = 1).columns
    kpis = ['n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca',	'diferenca_abs']

    for i in list(range(2, len(colunas_perfil))):
        df[coluna_contagem] = df[coluna_contagem].astype('int')
        dfr = df.iloc[:,:i]
        #agregação por perfil
        col_agg1 = dfr.columns.to_list()
        dfr = dfr.join(df[[coluna_grupo, coluna_contagem]])
        agg1 = dfr.groupby(col_agg1).agg({coluna_contagem:['sum']})
        agg1.columns = agg1.columns.droplevel(0)
        agg1 = agg1.reset_index()
        agg1 = agg1.rename(columns = {'sum':'n_seg'})
        #agreagação por perfil e grupo
        col_agg2 = dfr.drop(coluna_contagem, axis = 1).columns.to_list()
        agg2 = dfr.groupby(col_agg2).agg({coluna_contagem:['sum']})
        agg2.columns = agg2.columns.droplevel(0)
        agg2 = agg2.reset_index()
        agg2 = agg2.rename(columns = {'sum':'n_seg_grupo'})
        # agregação por consolidado grupo
        agg3= df.groupby(coluna_grupo).agg({coluna_contagem:['sum']})
        agg3.columns = agg3.columns.droplevel(0)
        agg3 = agg3.reset_index()
        agg3 = agg3.rename(columns = {'sum':'n_grupo'})
        # levar agg1 para agg2
        dfr2 = agg2.join(agg1.set_index(col_agg1), on = col_agg1, how = 'left')
        dfr2 = dfr2.join(agg3.set_index(coluna_grupo), on = coluna_grupo, how = 'left')
        total = agg3['n_grupo'].sum()
        # criar percentuais
        dfr2['total'] = total
        dfr2['per_grupo_seg'] = dfr2.n_seg_grupo / dfr2.n_seg
        dfr2['per_grupo_total'] = dfr2.n_grupo / dfr2.total
        dfr2['diferenca'] = dfr2['per_grupo_seg'] - dfr2['per_grupo_total'] 
        dfr2['diferenca_abs'] = np.abs(dfr2['diferenca']) 
        casos_signif = dfr2[dfr2['diferenca_abs'] > dif_var]
        casos_signif = casos_signif[casos_signif['n_seg_grupo'] > n_var]    
        casos_signif['segmento'] = ' '
        for i in casos_signif.drop(coluna_grupo, axis = 1).drop(kpis, axis = 1).columns:
            casos_signif[i].fillna("", inplace = True)
            casos_signif['segmento'] = casos_signif['segmento'] + ' - ' + casos_signif[i].astype("string")
        casos_signif = casos_signif[['segmento', 'n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca','diferenca_abs']].join(casos_signif[[coluna_grupo]])
        casos_signif['segmento'] = (casos_signif['segmento'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        casos_signif_final = casos_signif_final.append(casos_signif).sort_values('diferenca_abs', ascending = False)

    for i in colunas_perfil:   
        dfr = df[[i]]  
        dfr = dfr.join(df[[coluna_contagem, coluna_grupo]])

        #agregação por perfil
        agg1 = dfr.groupby(i).agg({coluna_contagem:['sum']})
        agg1.columns = agg1.columns.droplevel(0)
        agg1 = agg1.reset_index()
        agg1 = agg1.rename(columns = {'sum':'n_seg'})
        agg1.head()

        #agreagação por perfil e grupo
        col_agg2 = dfr.drop(coluna_contagem, axis = 1).columns.to_list()
        agg2 = dfr.groupby(col_agg2).agg({coluna_contagem:['sum']})
        agg2.columns = agg2.columns.droplevel(0)
        agg2 = agg2.reset_index()
        agg2 = agg2.rename(columns = {'sum':'n_seg_grupo'})

        # agregação por consolidado grupo
        agg3= df.groupby(coluna_grupo).agg({coluna_contagem:['sum']})
        agg3.columns = agg3.columns.droplevel(0)
        agg3 = agg3.reset_index()
        agg3 = agg3.rename(columns = {'sum':'n_grupo'})

        # levar agg1 para agg2
        dfr2 = agg2.join(agg1.set_index(i), on = i, how = 'left')
        dfr2 = dfr2.join(agg3.set_index(coluna_grupo), on = coluna_grupo, how = 'left')

        total = agg3['n_grupo'].sum()

        # criar percentuais
        dfr2['total'] = total
        dfr2['per_grupo_seg'] = dfr2.n_seg_grupo / dfr2.n_seg
        dfr2['per_grupo_total'] = dfr2.n_grupo / dfr2.total

        dfr2['diferenca'] = dfr2['per_grupo_seg'] - dfr2['per_grupo_total'] 
        dfr2['diferenca_abs'] = np.abs(dfr2['diferenca']) 
        casos_signif = dfr2[dfr2['diferenca_abs'] > dif_var]
        casos_signif = casos_signif[casos_signif['n_seg_grupo'] > n_var]
        casos_signif['segmento'] = ' '

        for i in casos_signif.drop(coluna_grupo, axis = 1).drop(kpis, axis = 1).columns:
            casos_signif[i].fillna("", inplace = True)
            casos_signif['segmento'] = casos_signif['segmento'] + ' - ' + casos_signif[i].astype("string")

        casos_signif = casos_signif[['segmento', 'n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca','diferenca_abs']].join(casos_signif[[coluna_grupo]])
        casos_signif['segmento'] = (casos_signif['segmento'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        casos_signif_final = casos_signif_final.append(casos_signif).sort_values('diferenca_abs', ascending = False)
        casos_signif_final.to_csv('report_perfil_total.csv')
        print('report dos perfis finalizado!')