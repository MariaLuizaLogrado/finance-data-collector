#%%
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

tickers = ["KNCR11.SA", "RZAK11.SA", "XPML11.SA", "TRXF11.SA", "TGAR11.SA"]
one_year_ago = datetime.now() - timedelta(days=365)

dfs = []
for ticker in tickers:
    fii = yf.Ticker(ticker)
    dividends = fii.dividends

    dividends.index = dividends.index.tz_localize(None)

    dividends_12m = dividends[dividends.index >= one_year_ago]

    df_div = dividends_12m.reset_index()
    df_div.columns = ["paymentDate", "dividend"]
    df_div['MEDIA_DIVIDENDOS'] = df_div.dividend.mean()
    df_div["ATIVO"] = ticker
    dfs.append(df_div)

df_concat = pd.concat(dfs)
df_grp = df_concat.groupby("ATIVO").agg({"paymentDate":"last", "MEDIA_DIVIDENDOS": "last", "dividend": "last"}).reset_index()


df_dividendos = df_grp.rename(columns={"paymentDate": "DATA_PAGAMENTO", "dividend": "DIVIDENDO"})

df_dividendos = df_grp["ATIVO"] = df_grp["ATIVO"].apply(lambda x: x.split('.SA')[0])
#%%
