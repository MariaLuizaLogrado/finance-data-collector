import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class ExtractDividendos:

    """
    Classe para buscar dividendos de ativos usando yfinance,
    processar e gerar DataFrame organizado para análise ou upload.
    """

    def __init__(self, tickers: dict, period_days: int = 365):
        """
        Inicializa a classe.

        Args:
            tickers (dict): dicionário {ticker: categoria} dos ativos.
            period_days (int): período em dias para considerar dividendos recentes.
        """
        self.tickers = tickers
        self.period_days = period_days
        self.one_year_ago = datetime.now() - timedelta(days=self.period_days)
        self.df_dividendos = pd.DataFrame()

    def fetch_dividends(self):
        """
        Busca dividendos de cada ticker e cria DataFrame base.
        """
        dfs = []

        for ticker, categoria in self.tickers.items():
            fii = yf.Ticker(ticker)
            dividends = fii.dividends

            dividends.index = dividends.index.tz_localize(None)

            dividends_12m = dividends[dividends.index >= self.one_year_ago]

            df_div = dividends_12m.reset_index()
            df_div.columns = ["paymentDate", "dividend"]
            df_div["MEDIA_DIVIDENDOS"] = df_div.dividend.mean()
            df_div["ATIVO"] = ticker
            df_div["CATEGORIA"] = categoria

            dfs.append(df_div)

        df_concat = pd.concat(dfs, ignore_index=True)
        self.df_dividendos = df_concat

    def process_dividends(self):
        """
        Agrupa e renomeia colunas, adiciona data de processamento.
        """
        if self.df_dividendos.empty:
            raise ValueError("DataFrame de dividendos está vazio. Execute fetch_dividends() primeiro.")

        df_grp = self.df_dividendos.groupby(["ATIVO", "CATEGORIA"]).agg({
            "paymentDate": "last",
            "MEDIA_DIVIDENDOS": "last",
            "dividend": "last"
        }).reset_index()

        df_dividendos = df_grp.rename(columns={
            "paymentDate": "DATA_PAGAMENTO",
            "dividend": "DIVIDENDO"
        })

        df_dividendos["ATIVO"] = df_dividendos["ATIVO"].apply(lambda x: x.split(".SA")[0])

        df_dividendos["PROCESSAMENTO"] = datetime.now()

        df_dividendos.sort_values(["CATEGORIA", "ATIVO", "DATA_PAGAMENTO"], inplace=True)
        self.df_dividendos = df_dividendos

    def get_dataframe(self) -> pd.DataFrame:
        """
        Retorna o DataFrame final de dividendos processado.
        """
        if self.df_dividendos.empty:
            raise ValueError("DataFrame ainda não processado. Execute fetch_dividends() e process_dividends() primeiro.")
        return self.df_dividendos.copy()
