from fastapi import FastAPI, UploadFile, File
from backend.services.query_service import get_metricas
from backend.routes.upload_routes import router as upload_router
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from backend.database.database import engine
import math
import os

app = FastAPI()

# 🔥 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔥 Detectar entorno
IS_RENDER = os.getenv("RENDER") is not None

UPLOAD_DIR = "/tmp/uploads" if IS_RENDER else "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app.include_router(upload_router)

# -------------------------------------

@app.get("/")
def home():
    return {"mensaje": "API funcionando"}

# -------------------------------------

@app.get("/datos")
def obtener_datos():

    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM datos LIMIT 100"))

        rows = []

        for row in result:
            fila = dict(row._mapping)

            for k, v in fila.items():
                if isinstance(v, float) and math.isnan(v):
                    fila[k] = None

            rows.append(fila)

    return {
        "total_mostrados": len(rows),
        "data": rows
    }

# -------------------------------------

@app.get("/metricas")
def metricas():
    return get_metricas()

# -------------------------------------

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):

    file_path = os.path.join(UPLOAD_DIR, file.filename)

    contents = await file.read()

    with open(file_path, "wb") as f:
        f.write(contents)

    return {
        "status": "ok",
        "archivo": file.filename
    }

# -------------------------------------

@app.get("/procesar")
def procesar():

    from backend.services.etl_service import procesar_excel

    archivos = os.listdir(UPLOAD_DIR)

    if not archivos:
        return {"error": "No hay archivos para procesar"}

    ruta = os.path.join(UPLOAD_DIR, archivos[0])

    registros = procesar_excel(ruta)

    return {
        "status": "procesado",
        "archivo": archivos[0],
        "total": len(registros)
    }

# -------------------------------------

@app.get("/resultados")
def resultados():

    with engine.connect() as conn:

        total = conn.execute(
            text("SELECT COUNT(*) FROM datos")
        ).scalar()

        columnas = conn.execute(
            text("PRAGMA table_info(datos)")
        ).fetchall()

        total_columnas = len(columnas)

    return {
        "dataset": "parametros_sicoe",
        "total_registros": total,
        "total_columnas": total_columnas
    }

# -------------------------------------

app.include_router(upload_router)