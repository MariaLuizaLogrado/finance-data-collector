#%%
from config.credentials import TOKEN_BRAPI, TOKEN, HOSTNAME, HTTP_PATH, TICKERS
from modules.extract_preco import ExtractPreco
from modules.easy_databricks import EasyDatabricks

print("Extraindo cotações..." )
fetcher = ExtractPreco(TICKERS, TOKEN_BRAPI)
fetcher.fetch_prices()
df_precos = fetcher.get_dataframe()

print("Salvando no Databricks...")
easy_databricks = EasyDatabricks(TOKEN, HOSTNAME, HTTP_PATH).bricks_connection()
easy_databricks.create_table(df_precos, 
                             schema_name = "investimentos", 
                             table_name = "cotacoes", 
                             mode = "overwrite")

print("Fim!")
#%%