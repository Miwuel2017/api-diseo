from fastapi import APIRouter, UploadFile, File
import shutil
import os
from backend.services.etl_service import procesar_excel

router = APIRouter()

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):

    ruta_guardado = os.path.join(UPLOAD_FOLDER, file.filename)

    with open(ruta_guardado, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    registros = procesar_excel(ruta_guardado)

    return {
        "mensaje": "Archivo cargado correctamente",
        "archivo": file.filename,
        "registros_insertados": registros
    }