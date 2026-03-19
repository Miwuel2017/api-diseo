import pandas as pd
from backend.database.database import engine

def save_dataframe(df, table_name):

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False
    )