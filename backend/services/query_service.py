from backend.database.database import engine
import pandas as pd


def get_all_data():

    query = "SELECT * FROM datos LIMIT 100"

    df = pd.read_sql(query, engine)

    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient="records")


def get_metricas():

    query = """
    SELECT 
        COUNT(*) as total_registros
    FROM datos
    """

    df = pd.read_sql(query, engine)

    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient="records")[0]