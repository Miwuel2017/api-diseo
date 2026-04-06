from fastapi import APIRouter, UploadFile, File
import os
from backend.services.etl_service import procesar_excel, obtener_year_desde_excel
from backend.services.query_service import year_ya_existe

router = APIRouter()

IS_RENDER = os.getenv("RENDER") is not None
UPLOAD_DIR = "/tmp/uploads" if IS_RENDER else "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_DIR, file.filename)

    try:
        print(f"📂 Guardando archivo en: {file_path}")

        with open(file_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)

        # 🔥 Validar año duplicado
        year = obtener_year_desde_excel(file_path)
        if year_ya_existe(year):
            return {
                "status": "duplicado",
                "msg": f"El año {year} ya fue cargado"
            }

        # 🔥 Procesar y guardar en DB
        registros = procesar_excel(file_path)

        return {
            "status": "ok",
            "archivo": file.filename,
            "registros": registros
        }

    except Exception as e:
        print("ERROR UPLOAD:", str(e))
        return {"error": str(e)}

