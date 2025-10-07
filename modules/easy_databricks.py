from databricks import sql
import pandas as pd

class EasyDatabricks:

    def __init__(self, TOKEN, HOSTNAME, HTTP_PATH):
        self.TOKEN = TOKEN
        self.HOSTNAME = HOSTNAME
        self.HTTP_PATH = HTTP_PATH


    def bricks_connection(self):
        self.connection = sql.connect(
                server_hostname=self.HOSTNAME,
                http_path=self.HTTP_PATH,
                access_token=self.TOKEN
            )
        
        self.cursor = self.connection.cursor()
        return self


    def create_table(self, df, schema_name, table_name, mode = "append"):
        """
        Salva um DataFrame pandas em uma tabela Databricks.
        
        Args:
            df: pandas DataFrame
            schema_name: nome do schema (ex: "investimentos")
            table_name: nome da tabela (ex: "dividendos")
            mode: "append" ou "overwrite"
        """

        # Cria schema se não existir
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Verifica se tabela existe
        self.cursor.execute(f"SHOW TABLES IN {schema_name}")
        existing_tables = [row[1] for row in self.cursor.fetchall()]
        table_exists = table_name in existing_tables

        if table_exists and mode == "overwrite":
            self.cursor.execute(f"DROP TABLE {schema_name}.{table_name}")
            table_exists = False  # força a criação de nova tabela

        # Cria tabela se não existir
        if not table_exists:
            # Detecta tipos básicos automaticamente
            col_defs = []
            for col, dtype in zip(df.columns, df.dtypes):
                if "object" in str(dtype):
                    col_defs.append(f"{col} STRING")
                elif "float" in str(dtype):
                    col_defs.append(f"{col} FLOAT")
                elif "int" in str(dtype):
                    col_defs.append(f"{col} INT")
                elif "datetime" in str(dtype):
                    col_defs.append(f"{col} DATE")
                else:
                    col_defs.append(f"{col} STRING")
            col_defs_str = ", ".join(col_defs)
            self.cursor.execute(f"CREATE TABLE {schema_name}.{table_name} ({col_defs_str})")

        # Insere dados
        for _, row in df.iterrows():
            values = []
            for col, dtype in zip(df.columns, df.dtypes):
                val = row[col]
                if "object" in str(dtype) or "datetime" in str(dtype):
                    # datas -> string YYYY-MM-DD
                    if pd.notnull(val) and "datetime" in str(dtype):
                        val = val.strftime("%Y-%m-%d")
                    val = f"'{val}'"
                elif pd.isnull(val):
                    val = "NULL"
                values.append(str(val))
            values_str = ", ".join(values)
            self.cursor.execute(f"INSERT INTO {schema_name}.{table_name} VALUES ({values_str})")

        self.connection.close()
        print(f"Tabela {schema_name}.{table_name} atualizada com sucesso!")
        
