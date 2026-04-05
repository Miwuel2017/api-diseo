from fastapi import APIRouter, UploadFile, File
import os
from backend.services.etl_service import procesar_excel

router = APIRouter()

IS_RENDER = os.getenv("RENDER") is not None
UPLOAD_FOLDER = "/tmp/uploads" if IS_RENDER else "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)

    try:
        print(f"📂 Guardando archivo en: {file_path}")

        with open(file_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)

        registros = procesar_excel(file_path)

        return {
            "status": "ok",
            "archivo": file.filename,
            "ruta": file_path,
            "registros_insertados": registros
        }

    except Exception as e:
        print("ERROR UPLOAD:", str(e))
        return {"error": str(e)}

