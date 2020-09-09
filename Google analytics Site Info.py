import pandas as pd
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

#PARAMETROS
import os
#COLOCAR A PASTA ONDE ESTÁ O JSON COM AS CREDENCIAIS
workdir_path = 'C:/Users/FlaviaCosta/Google Drive/Conselho_Flávia/01 - Planilhas de Apoio'
os.chdir(workdir_path)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'client_secrets.json'

#VIEWS QUE SÃO IMPORTADAS
VIEWS = ['104345127','94578507', '104219385']

#DATAS CALCULADAS
from datetime import datetime
from dateutil.relativedelta import relativedelta
today = datetime.today().strftime('%Y-%m-%d')
today_ma = (datetime.today() + relativedelta(months=-1))
today_ma_str = today_ma.strftime('%Y-%m-%d') #dia da extração mes anterior
since = today_ma_str[0:7] + "-01" #mes anterior - dia primeiro
until_aux = last_date_of_month = datetime(today_ma.year, today_ma.month, 1) + relativedelta(months=1, days=-1)
until = until_aux.strftime('%Y-%m-%d')

#INPUT - DATAS

#START_DATE = '2020-07-01'
#END_DATE   = '2020-07-31'
START_DATE = since
END_DATE = until

# For the full list of dimensions & metrics, check https://developers.google.com/analytics/devguides/reporting/core/dimsmets
DIMENSIONS = ['ga:devicecategory']
METRICS    = ['ga:users','ga:sessions', 'ga:avgSessionDuration']

#INICIANDO AS FUNÇÕES

def initialize_analyticsreporting():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

def get_report(analytics):
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID
         ,  'dateRanges': [{'startDate': START_DATE, 'endDate': END_DATE}]
         ,  'metrics': [{'expression':i} for i in METRICS]
         ,  'dimensions': [{'name':j} for j in DIMENSIONS]
         , 'filtersExpression':"ga:landingPagePath!@carriernoscrolltim;ga:landingPagePath!@carrier/;ga:landingPagePath!@ubooknotrial;ga:landingPagePath!@assinaturaMobile/timweb;ga:sourceMedium!=afiliates / cpa"
        }]
      }
  ).execute()


def convert_to_dataframe(response):
    
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = [i.get('name',{}) for i in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
    finalRows = []
    

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      metrics = row.get('metrics', [])[0].get('values', {})
      rowObject = {}

      for header, dimension in zip(dimensionHeaders, dimensions):
        rowObject[header] = dimension
        
        
      for metricHeader, metric in zip(metricHeaders, metrics):
        rowObject[metricHeader] = metric

      finalRows.append(rowObject)
        
  dataFrameFormat = pd.DataFrame(finalRows)    
  return dataFrameFormat  

def export_df(df):
    df.to_csv("export_data_view_" + VIEW_ID + "_" + END_DATE + ".csv")   
      
def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    df = convert_to_dataframe(response)
    export_df(df)
    print('salva view_' + VIEW_ID)                             

for v in range(0, len(VIEWS)):
    VIEW_ID = VIEWS[v]
    if __name__ == '__main__':
     main()