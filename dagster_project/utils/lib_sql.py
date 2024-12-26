from dataclasses import dataclass
import os
import pandas as pd
import sqlalchemy as sql
from dotenv import load_dotenv

load_dotenv()

@dataclass
class pg_connect:
    PG_UID: str = os.getenv('PG_UID')
    PG_PWD: str = os.getenv('PG_PWD')
    PG_SERVER: str = os.getenv('PG_SERVER')
    PG_PORT: str = os.getenv('PG_PORT', '5432')
    PG_DB: str = os.getenv('PG_DB')

    def create_connection(self, **kwargs):
        connection = sql.create_engine(
            f"""postgresql+psycopg2://{self.PG_UID}:{self.PG_PWD}@{self.PG_SERVER}:{self.PG_PORT}/{self.PG_DB}"""
        )
        return connection

    def read_data(self, query: str, **kwargs):
        sa_engine = self.create_connection(**kwargs)
        sqlalchemy_query = sql.text(query)
        with sa_engine.connect() as connection:
            result = connection.execute(sqlalchemy_query)
            df_output = pd.DataFrame(result.fetchall(), columns=result.keys())
        connection.close()
        return df_output

    def bulk_update(self, df, **kwargs):
        sa_engine = self.create_connection(**kwargs)
        table_name = kwargs.get('tableName')
        schema = kwargs.get('schema', 'public')
        if not table_name:
            raise ValueError("tableName is required for bulk_update.")
        df.to_sql(
            table_name,
            sa_engine,
            schema=schema,
            if_exists="append",
            index=False
        )
        return True
    
    def execute(self, query : str, **kwargs):
        sa_engine = self.create_connection(**kwargs)
        sql_query = sql.text(query)
        try:
            with sa_engine.connect() as connection:
                connection.execute(sql_query)
                connection.commit()  # Commit the transaction if needed
            return True
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}")
        finally:
            sa_engine.dispose()