from services.drive_service import list_files_in_folder, download_file
from services.db_service import save_dataframe
import pandas as pd

FOLDER_ID = "1D24XM9S4TFBsyKfx1MY39MDNpNy2soZD"  # Reemplaza con tu ID de carpeta en Google Drive

files = list_files_in_folder(FOLDER_ID)

for file in files:

    print("Procesando:", file["name"])

    file_stream = download_file(file["id"], file["mimeType"])

    df = pd.read_excel(file_stream)

    print("Registros:", len(df))

    save_dataframe(df, "datos")

    print("Datos guardados en DB")