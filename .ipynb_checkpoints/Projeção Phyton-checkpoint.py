# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 15:14:17 2020

@author: FlaviaCosta
"""
#%% Install library
#pip install statsmodels
#%% import library
import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols
#%% importando base excel
base_projecao = pd.read_excel('C:/Users/FlaviaCosta/Google Drive/Projeções/Projeções.xlsm'
                              , index_col = 0, sheet_name = 'BASE MODELAGEM PHYTON')
#%% Data para base treino
datateste = '2020-04-01'
#%%
base_projecao['Inicio_Teste'] = pd.to_datetime(datateste)
#%% separando base treino e teste
base_treino = base_projecao.query('Data < Inicio_Teste')
base_teste  = base_projecao.query('Data >= Inicio_Teste')

#%% definindo variáveis do modelo
formula_text = ("""
    LOG_GUV ~ 
    C(month)
    + ITUNES
    + VIVOBUNDLECOMBO
    + TIM
    + OIBUNDLELIVROSNARRADOS
    + CARTAODECREDITO
    + CLARO
    + GOOGLEPLAY
    + VIVO
    + NEXTEL
    + OI
    + ALGARBUNDLE
    + MOVISTARESPANHA
    + OIUPSTREAM
    + TIGOCOLOMBIA
    + OUTROS
    + Outlier

    """)
#%% exemplo incluindo variavel como categorica
#res = ols(formula='Lottery ~ Literacy + Wealth + C(Region)', data=df).fit()
#vai retornar um coeficiente para cada região
#%% Ajustando o modelo  
model = ols(formula = formula_text, data = base_treino).fit()
#%% parametros
print(model.summary())
#%% gráficos - tranformar em numpy array para colocar num gráfico
import numpy as np
Ypred = np.array(np.exp(model.fittedvalues))
Yreal = np.array(np.exp(base_treino['LOG_GUV']))
#%% Gráfico
import matplotlib.pyplot as plt
plt.plot(Ypred, color = 'red', label = 'Real')
plt.plot(Yreal, color = 'blue', label = 'Previsto')
plt.legend()
plt.title('Previsão de série temporal')
#%% predição
prediction = np.exp(model.predict(base_teste))
#%%Unificando predição total
predictionTotal = np.append(Ypred, np.array(prediction))
#%% export predictions
pd.DataFrame(predictionTotal).to_csv('C:/Users/FlaviaCosta/Google Drive/Projeções/projecao_faturamento.csv')

#%% export coeficients
coeficientes = np.array(model.params)
pd.DataFrame(coeficientes).to_csv('C:/Users/FlaviaCosta/Google Drive/Projeções/coeficientes.csv')