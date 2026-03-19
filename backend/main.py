from fastapi import FastAPI
from fastapi import UploadFile, File
from backend.services.query_service import get_all_data, get_metricas
from backend.routes.upload_routes import router as upload_router
from fastapi.middleware.cors import CORSMiddleware
from backend.routes import upload_routes
from sqlalchemy import create_engine, text
from backend.database.database import engine
import math

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(upload_router)

@app.get("/")
def home():
    return {"mensaje": "API funcionando"}




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

@app.get("/metricas")
def metricas():
    return get_metricas()

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):

    os.makedirs("uploads", exist_ok=True)  # 👈 clave

    contents = await file.read()

    with open(f"uploads/{file.filename}", "wb") as f:
        f.write(contents)

    return {
        "status": "ok",
        "archivo": file.filename
    }

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


app.include_router(upload_routes.router)