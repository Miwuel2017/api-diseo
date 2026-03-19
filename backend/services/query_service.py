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

def year_ya_existe(year):
    from sqlalchemy import text
    from backend.database.database import engine

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM datos WHERE year = :year"),
            {"year": year}
        ).scalar()

    return result > 0