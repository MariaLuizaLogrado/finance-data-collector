# from config.credentials import TOKEN, HOSTNAME, HTTP_PATH, TICKERS
from modules.easy_databricks import EasyDatabricks
from modules.extract_dividendos import ExtractDividendos
import os, json


TOKEN = os.getenv("TOKEN")
HOSTNAME = os.getenv("HOSTNAME")
HTTP_PATH = os.getenv("HTTP_PATH")
TICKERS = json.loads(os.getenv("TICKERS"))

print("Extraindo dividendos...")
fetcher = ExtractDividendos(TICKERS)
fetcher.fetch_dividends()
fetcher.process_dividends()
df_dividendos = fetcher.get_dataframe()

print("Salvando no Databricks...")
easy_databricks = EasyDatabricks(TOKEN, HOSTNAME, HTTP_PATH).bricks_connection()
easy_databricks.create_table(df_dividendos, 
                             schema_name = "investimentos", 
                             table_name = "dividendos", 
                             mode = "append")

print("Fim!")