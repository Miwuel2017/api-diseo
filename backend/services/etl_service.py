import pandas as pd
from backend.database.database import engine


def limpiar_columnas(df):
    """
    Limpia nombres de columnas:
    - quita espacios
    - reemplaza espacios por _
    - convierte a minúsculas
    """
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace(" ", "_")
        .str.replace(".", "_")
        .str.lower()
    )
    return df


def procesar_excel(ruta_archivo):

    excel = pd.ExcelFile(ruta_archivo)

    dataframes = []
    columnas_base = None

    for i, hoja in enumerate(excel.sheet_names):

        try:
            df = pd.read_excel(ruta_archivo, sheet_name=hoja)

            # Ignorar hojas vacías
            if df.empty:
                continue

            # Primera hoja define estructura
            if columnas_base is None:
                df = limpiar_columnas(df)
                columnas_base = df.columns
            else:
                df.columns = columnas_base

            dataframes.append(df)

        except Exception as e:
            print(f"Error leyendo hoja {hoja}: {e}")

    if not dataframes:
        raise Exception("El archivo Excel no contiene datos válidos")

    # Unir todas las hojas
    df_final = pd.concat(dataframes, ignore_index=True)

    # Eliminar columnas duplicadas
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]

    # Eliminar filas completamente vacías
    df_final = df_final.dropna(how="all")

    # Guardar en base de datos
    df_final.to_sql(
        "datos",
        engine,
        if_exists="replace",
        index=False,
        chunksize=10000
    )

    return len(df_final)