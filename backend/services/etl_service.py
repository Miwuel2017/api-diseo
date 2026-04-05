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

    for hoja in excel.sheet_names:

        try:
            df = pd.read_excel(ruta_archivo, sheet_name=hoja)

            # 🔥 limpiar columnas SIEMPRE
            df.columns = df.columns.str.strip()

            # 🔥 ignorar hojas vacías
            if df.empty or df.shape[1] == 0:
                continue

            # 🔥 validar columna clave
            if "Fechaing" not in df.columns:
                print(f"Hoja {hoja} ignorada: no tiene 'Fechaing'")
                continue

            # 🔥 convertir fecha
            df["Fechaing"] = pd.to_datetime(
                df["Fechaing"],
                errors="coerce",
                dayfirst=True
            )

            # eliminar filas sin fecha válida
            df = df.dropna(subset=["Fechaing"])

            if df.empty:
                continue

            # 🔥 agregar año
            df["year"] = df["Fechaing"].dt.year

            # 🔥 estructura base
            if columnas_base is None:
                columnas_base = df.columns
            else:
                df = df[columnas_base]

            dataframes.append(df)

        except Exception as e:
            print(f"Error en hoja {hoja}: {e}")

    # 🔥 VALIDACIÓN FINAL
    if not dataframes:
        raise Exception("El archivo no tiene datos válidos o no contiene la columna Fechaing")

    df_final = pd.concat(dataframes, ignore_index=True)

    df_final = df_final.dropna(how="all")

    # 🔥 guardar en BD
    df_final.to_sql(
        "datos",
        engine,
        if_exists="append",  # 🔥 cambiar de replace a append
        index=False,
        chunksize=1000
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