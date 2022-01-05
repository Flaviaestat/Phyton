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


def dados(usuario, df, campocpf):
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
                select * from (
                select 
                p.cpf as chave_or,
                p.ds_origin,
                nm_customer as ct_nome_cliente,
                rank() over (partition by p.cd_cpf order by dt_cfi_account_created desc) as base_contato,
		email as ct_email,
		nr_phone as ct_celular
                from growth_curated_zone.customer  p
                ) where base_contato = 1
    """
    
    df_ct = pd.read_sql(query, engine_athena)
    df_ct['chave'] = df_ct['chave_or'].astype('string').str.zfill(11)
    df['chave'] = df[campocpf].astype('string').str.zfill(11)
    df = df.join(df_ct.set_index('chave'), how = 'left', on = 'chave')
    return df


