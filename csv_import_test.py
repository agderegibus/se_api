import pandas as pd

URL = ("https://github.com/agderegibus/se_api/blob/main/ListadoCIM.csv?raw=true")

df = pd.read_csv(URL,index_col=0,encoding='latin-1')

print(df.head())