from fastapi import FastAPI, UploadFile, File
from backend.services.query_service import get_metricas
from backend.routes.upload_routes import router as upload_router
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from backend.database.database import engine
import math
import os
from backend.services.query_service import year_ya_existe
from backend.routes.dashboard_routes import router as dashboard_router



app = FastAPI()

app.include_router(dashboard_router)

# 🔥 CORS
allowed_origins = [origin.strip() for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173,https://n-pgsqj5lhd6tq7d5bnzpqljzmdtmu5naqfo7yzsy-0lu-script.googleusercontent.com").split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
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

@app.get("/cors-test")
def cors_test():
    return {"allowed_origins": allowed_origins}

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

@app.get("/procesar")
def procesar():
    try:
        import os
        from backend.services.etl_service import procesar_excel, obtener_year_desde_excel
        from backend.services.query_service import year_ya_existe

        UPLOAD_DIR = "/tmp/uploads"

        archivos = os.listdir(UPLOAD_DIR)

        if not archivos:
            return {"error": "No hay archivos para procesar"}

        ruta = os.path.join(UPLOAD_DIR, archivos[0])

        # 🔥 1. sacar año del excel
        year = obtener_year_desde_excel(ruta)

        # 🔥 2. validar duplicado
        if year_ya_existe(year):
            return {
                "status": "duplicado",
                "msg": f"El año {year} ya fue cargado"
            }

        # 🔥 3. procesar
        registros = procesar_excel(ruta)

        return {
            "status": "procesado",
            "year": year,
            "total": len(registros)
        }

    except Exception as e:
        print("ERROR /procesar:", str(e))
        return {"error": str(e)}
# -------------------------------------
@app.get("/years")
def get_years():
    try:
        with engine.connect() as conn:

            # 🔥 validar si existe tabla
            table_check = conn.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_name = 'datos'")
            ).fetchone()

            if not table_check:
                return {"years": []}

            result = conn.execute(
                text("SELECT DISTINCT year FROM datos WHERE year IS NOT NULL ORDER BY year DESC")
            )

            years = [row[0] for row in result]

        return {"years": years}

    except Exception as e:
        print("ERROR /years:", str(e))
        return {"years": []}
# -------------------------------------

@app.get("/resultados")
def resultados():

    with engine.connect() as conn:

        total = conn.execute(
            text("SELECT COUNT(*) FROM datos")
        ).scalar()

        columnas = conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name = 'datos'")
        ).fetchall()

        total_columnas = len(columnas)

    return {
        "dataset": "parametros_sicoe",
        "total_registros": total,
        "total_columnas": total_columnas
    }

# -------------------------------------