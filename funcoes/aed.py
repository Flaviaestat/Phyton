import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def find_correl(df, id_col, threshold):
    cat_columns = df.drop(id_col, axis=1).select_dtypes('object').columns
    corr = pd.get_dummies(df.drop(id_col, axis=1), columns=cat_columns)
    corr = corr.dropna(how='all')
    corr2 = corr.corr(method='spearman')
    corr2.reset_index(level=0, inplace=True)
    for column in corr2.drop('index', axis=1).columns:
        for i in range(0, len(corr2)):
            celula = corr2[column][i]
            if abs(celula) != 1 and abs(celula) > threshold:
                print(column + " ---> " + str(corr2['index'][i]) + " -> correlacao de " + str(celula))
                print("__________________________________________")
            else:
                pass

def vi(dataset, id_col, var_interesse):
    cat_columns = dataset.drop(id_col, axis=1).select_dtypes('object').columns
    dt = pd.get_dummies(dataset, columns=cat_columns)
    avg_num = dt.drop(id_col, axis=1).groupby(var_interesse, as_index=False).mean()
    display(avg_num)
    
    for column in cat_columns:
        avg_cat = dataset[[column,var_interesse]].groupby(column, as_index=False).mean()
        a4_dims = (20, 5)
        fig, ax = plt.subplots(figsize=a4_dims)
        sns.barplot(ax=ax, x = var_interesse, y = column, data = avg_cat)
        locs, labels = plt.xticks()
        plt.setp(labels, rotation=90)
    for column in cat_columns:
        plt.figure(figsize = (10,5))
        sns.catplot(y=column, hue=var_interesse, data=dataset, kind="count")
        plt.show()


#valida as variáveis
def valid(dataset, id_columns):
    print("tamanho do dataset:")
    print(len(dataset))
    print("valores nulos:")
    print(dataset.isnull().sum())
    cat_columns = dataset.drop(id_columns, axis = 1).select_dtypes('object').columns
    num_columns = dataset.drop(id_columns, axis = 1).select_dtypes(exclude=['object']).columns
    stats = dataset.drop(id_columns, axis=1).describe()
    print("valores distintos das variaveis categoricas:")
    for column in cat_columns:
        
        print(column)
        print(len(set(dataset[column])))
    print("____________________________")
    #num_outlier = []
    
    for i in num_columns:
        p75 = stats.iloc[6][i]
        p25 = stats.iloc[4][i]
        iqr = p75 - p25
        higher_outlier = p75 + (1.5 * iqr)
        print(i)
        
        print("mínimo:")
        print(stats.iloc[3][i])
        
        print("máximo:")
        print(stats.iloc[7][i])
        
        print("media:")
        print(stats.iloc[1][i])
        
        print("mediana:")
        print(stats.iloc[5][i])
        
        print("coeficiente de variação:")
        print((stats.iloc[2][i] / stats.iloc[1][i])*100)
        
        print("coeficiente de assimetria de pearson:")
        cap = ((stats.iloc[1][i] - stats.iloc[5][i]) * 3) / stats.iloc[2][i]
        print(cap)
        if cap>0:
            print("assimétrica positiva - concentração em valores menores")
        elif cap<0: 
            print("assimétrica negativa - concentração em valores maiores")
                
        print("quantidade de outliers:")
        print((dataset[i] > higher_outlier).sum())
        print("quantidade de outliers percentual:")
        print(round((dataset[i] > higher_outlier).sum()/len(dataset) * 100,2))
        
        print("____________________________")
      #num_outlier.append(count_outlier)
        
    
    #print(num_columns)
    #print(num_outlier)
 
#discretiza todas as variáveis numéricas
def discretiza(df, drop_col):
    num_columns = df.drop(drop_col, axis = 1).select_dtypes(exclude=['object']).columns
    stats = df.drop(drop_col, axis=1).describe()
    for i in num_columns:
        p75 = stats.iloc[6][i]
        p25 = stats.iloc[4][i]
        p50 = stats.iloc[5][i]
        
        df['aux'] = np.where((df[i] < p25), "A",
                    np.where((df[i] > p25) & (df[i] < p50), "B",
                    np.where((df[i] > p50) & (df[i] < p75), "C", "D")))
  
        df = df.rename(columns = {'aux':i+"_fx"})

    return df

def tira_outlier(df, cols_outlier):
    sem_outlier = df
    stats = sem_outlier.describe()
    for i in cols_outlier:
        p75 = stats.iloc[6][i]
        p25 = stats.iloc[4][i]
        iqr = p75 - p25
        higher_outlier = p75 + (5* iqr)
        sem_outlier = sem_outlier[sem_outlier[i] < higher_outlier]
        sem_outlier = sem_outlier[sem_outlier[i] > 0]
        return sem_outlier
  