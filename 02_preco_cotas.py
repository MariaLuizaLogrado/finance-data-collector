#%%
from config.credentials import TOKEN
import requests
import pandas as pd

tickers = ["KNCR11", "RZAK11", "XPML11", "TRXF11", "TGAR11"]

dfs = []
for ticker in tickers:
    url = f'https://brapi.dev/api/quote/{ticker}?token={TOKEN}'

    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data["results"])
    precos = df[['symbol', 'regularMarketPreviousClose', 'regularMarketTime']]
    dfs.append(precos)

df_precos = pd.concat(dfs)
df_precos.rename(columns={"symbol": "ATIVO", "regularMarketPreviousClose": "PRECO_FECHAMENTO", "regularMarketTime": "DATA"}, inplace=True)
df_precos["DATA"] = pd.to_datetime(df_precos["DATA"].str[:10], format='%Y-%m-%d')
df_precos
#%%