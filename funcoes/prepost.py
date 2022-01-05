import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats


def stats(df, col1, col2):
    var = [col1, col2]
    sem_outlier = df
    stats = sem_outlier.describe()
    for i in var:
        p75 = stats.iloc[6][i]
        p25 = stats.iloc[4][i]
        iqr = p75 - p25
        higher_outlier = p75 + (5* iqr)
        sem_outlier = sem_outlier[sem_outlier[i] < higher_outlier]
        sem_outlier = sem_outlier[sem_outlier[i] > 0]
    print("Quanto a base sem outliers representa do total: "   + str(len(sem_outlier) / len(df) * 100) + "%")
    print("____________________")
    print("Resumo dos campos:")
    print(sem_outlier[[col1,col2]].describe())
    print("____________________")
    from scipy import stats
    t, p = stats.ttest_rel(sem_outlier[col2], sem_outlier[col1])
    print("Estatística t: "+str(round(t,2)))
    print("P-valor: "+str(round(p,6)))
    if p >0.05:
        print("A variação entre o período anterior e posterior não é significativa")
    else:
        print("A variação entre o período anterior e posterior é estatisticamente significativa")

    print("Variação: " + 
    str(round((np.mean(sem_outlier[col2])/np.mean(sem_outlier[col1]))-1, 2)*100)
        +"%")
    plt.hist(sem_outlier[col1], bins = 20, alpha = 0.3, label='anterior')
    plt.hist(sem_outlier[col2], bins = 20, alpha = 0.3, label='atual')
    plt.legend(loc='upper right')
    plt.title('Antes x Depois')
    plt.show()
    
def segmenta(df, col1, col2):
    df['seg_pre_post'] = np.where((df[col1] == 0) & (df[col2] > 0), "presente apenas no período posterior",
                         np.where((df[col1] > 0) & (df[col2] == 0), "presente apenas no período anterior",
                         np.where(df[col2] >=  df[col1], "aumentou no período posterior",
                         "reduziu no período posterior")))
    print(df['seg_pre_post'].value_counts())
    print(df['seg_pre_post'].value_counts(normalize = True))
    df = pd.concat([df['seg_pre_post'], df], axis=1)