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

    import pandas as pd
    from backend.database.database import engine

    excel = pd.ExcelFile(ruta_archivo)

    dataframes = []
    columnas_base = None

    for i, hoja in enumerate(excel.sheet_names):

        try:
            df = pd.read_excel(ruta_archivo, sheet_name=hoja)

            if df.empty:
                continue

            # 🔥 LIMPIAR COLUMNAS SOLO EN LA PRIMERA
            if columnas_base is None:
                df = limpiar_columnas(df)
                columnas_base = df.columns
            else:
                df.columns = columnas_base

            # 🔥 VALIDAR Fechaing
            if "Fechaing" not in df.columns:
                raise ValueError("No existe columna Fechaing")

            # 🔥 CONVERTIR FECHA
            df["Fechaing"] = pd.to_datetime(df["Fechaing"], errors="coerce", dayfirst=True)

            # 🔥 AGREGAR YEAR (AQUÍ ES DONDE DEBE IR)
            df["year"] = df["Fechaing"].dt.year

            dataframes.append(df)

        except Exception as e:
            print(f"Error leyendo hoja {hoja}: {e}")

    if not dataframes:
        raise Exception("El archivo Excel no contiene datos válidos")

    # 🔥 UNIR TODO
    df_final = pd.concat(dataframes, ignore_index=True)

    # 🔥 LIMPIEZAS
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    df_final = df_final.dropna(how="all")

    # 🔥 GUARDAR
    df_final.to_sql(
        "datos",
        engine,
        if_exists="replace",
        index=False,
        chunksize=10000
    )

    return len(df_final)

def obtener_fecha_archivo(ruta_archivo):
    df = pd.read_excel(ruta_archivo, header=None)

    # fila 2 = índice 1
    # columna E = índice 4
    fecha = df.iloc[1, 4]

    return str(fecha)

def obtener_year_desde_excel(ruta_archivo):

    df = pd.read_excel(ruta_archivo)

    # 🔴 Validación
    if "Fechaing" not in df.columns:
        raise ValueError("El archivo no contiene la columna 'Fechaing'")

    # 🔥 Convertir a fecha
    df["Fechaing"] = pd.to_datetime(df["Fechaing"], errors="coerce", dayfirst=True)

    # 🔥 Tomar la primera fecha válida
    fecha_valida = df["Fechaing"].dropna().iloc[0]

    year = int(fecha_valida.year)

    return year