import requests
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class ExtractPreco:
    """
    Classe para buscar preços de ativos usando a API Brapi e processar em DataFrame pandas.
    """

    def __init__(self, tickers: dict, token: str):

        self.tickers = tickers
        self.token = token
        self.df_precos = pd.DataFrame()

    def fetch_prices(self):

        dfs = []

        for ticker, categoria in self.tickers.items():
            url = f'https://brapi.dev/api/quote/{ticker}?token={self.token}'
            response = requests.get(url)
            
            if response.status_code != 200:
                print(f"Atenção: não foi possível buscar {ticker}. Status code: {response.status_code}")
                continue

            data = response.json()
            df = pd.DataFrame(data.get("results", []))
            
            if df.empty:
                print(f"Atenção: sem dados para {ticker}.")
                continue

            # Seleciona colunas importantes
            precos = df[['symbol', 'regularMarketPreviousClose', 'regularMarketTime']].copy()
            precos["CATEGORIA"] = categoria
            dfs.append(precos)

        if dfs:
            # Concatena todos os DataFrames
            df_precos = pd.concat(dfs, ignore_index=True)

            # Renomeia colunas
            df_precos.rename(columns={
                "symbol": "ATIVO",
                "regularMarketPreviousClose": "PRECO_FECHAMENTO",
                "regularMarketTime": "DATA_PROCESSAMENTO"
            }, inplace=True)

            # Converte datas para datetime
            df_precos["DATA_PROCESSAMENTO"] = pd.to_datetime(
                df_precos["DATA_PROCESSAMENTO"].astype(str).str[:10],
                format='%Y-%m-%d'
            )

            df_precos["ATIVO"] = df_precos["ATIVO"].apply(lambda x: x.split(".SA")[0])

            # Armazena no atributo da classe
            self.df_precos = df_precos
        else:
            print("Nenhum dado foi retornado pela API.")

    def get_dataframe(self) -> pd.DataFrame:


        if self.df_precos.empty:
            raise ValueError("DataFrame vazio. Execute fetch_prices() primeiro.")
        return self.df_precos.copy()
