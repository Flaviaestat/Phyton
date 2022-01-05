import pandas as pd
import numpy as np
import os
import boto3
import io
from io import StringIO
from urllib.parse import quote_plus  # PY2: from urllib import quote_plus
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Table, MetaData


################ S3 AMAZON PAG VARS
ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID_PAG') 
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY_PAG')
BUCKET_NAME = 'datalake-grupoavista'
REGION = 'us-east-1'

#função para leitura de arquivo no s3
def read_from_s3(KEY, delim):
    s3c = boto3.client(
            's3', 
            region_name = REGION,
            aws_access_key_id = ACCESS_KEY_ID,
            aws_secret_access_key = SECRET_ACCESS_KEY
        )

    obj = s3c.get_object(Bucket= BUCKET_NAME , Key = KEY)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8', delimiter = delim)
    return df

def df_athena(usuario, query):
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
    df = pd.read_sql(query, engine_athena)
    return df


#def upload_athena(usuario, df, nometabela):
#    ACCESS_KEY_ID_WILL = os.getenv('AWS_ACCESS_KEY_ID_WILL')
#    SECRET_ACCESS_KEY_WILL = os.getenv('AWS_SECRET_ACCESS_KEY_WILL')
#    STAGING_DIR = 's3://data-athena-query-result-will-prod/' + usuario
#    SCHEMA = usuario
#    con1 = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"
#    con2 = "{schema_name}?s3_staging_dir={s3_staging_dir}"
#    conn_str = con1 + con2
#    engine_athena = create_engine(conn_str.format(
#        aws_access_key_id=quote_plus(ACCESS_KEY_ID_WILL),
#        aws_secret_access_key=quote_plus(SECRET_ACCESS_KEY_WILL),
#        region_name="sa-east-1",
#        schema_name=SCHEMA,
#        s3_staging_dir=quote_plus(STAGING_DIR)))
#    df.to_sql(nometabela, con=engine_athena, if_exists='append', index=False)
#    print('Upload bem sucedido da tabela: ' + nometabela + '!!')


#Como usar a função:
# 1 - criar uma var com a query (query = )
# 2 - usar o comando abaixo já vai fornecer o dtframe
#base = df_athena('cynthia-vianna', query)

def df_redshift(query):
    user_redshift = os.getenv('USER_REDSHIFT')
    senha_redshift = os.getenv('SENHA_REDSHIFT')
    str_conn = 'postgresql://'+user_redshift+":"+senha_redshift+"@datalake-cluster.ckkb9lvch2lp.us-east-1.redshift.amazonaws.com:5439/grupoavista"
    engine_redshift = create_engine(str_conn)
    engine_redshift.execute("""
                        
                        DROP TABLE IF EXISTS public.base_temp_cartoes;

                         """, engine_redshift)
    
    engine_redshift.execute("CREATE TABLE public.base_temp_cartoes AS " + """ """ + query)
    
    
    engine_redshift.execute("""
                        unload(
                        $$
                        select * from public.base_temp_cartoes
                        $$
                        )
                        TO 's3://datalake-grupoavista/cartoes/base_temp.csv'
                        iam_role 'arn:aws:iam::739007973549:role/RedShift-S3FullAccess'
                        HEADER
                        DELIMITER ';'
                        PARALLEL OFF
                        ALLOWOVERWRITE                 
                         """)
    path_to_csv = 'cartoes/base_temp.csv000'
    df = read_from_s3(path_to_csv, ';')
    return df


#função para salvar arquivo no s3
def save_to_s3(path, filename, df, delim):
    
    client = boto3.client(
        's3', region_name = REGION,
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY)
   
    csv_buffer=StringIO()
    df.to_csv(csv_buffer, index = False, sep = delim)
    content = csv_buffer.getvalue()
    
    response = client.put_object( 
    Bucket=BUCKET_NAME,
    Body= content,
    Key=path+filename
    )

#filename = 'streaming_202109.csv'
#df= s3_pag
#path = 'marketing/bases_campanha/2021/'
#save_to_s3(path, filename, df, delim = ';')   
