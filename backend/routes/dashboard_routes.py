from fastapi import APIRouter
from sqlalchemy import text
from backend.database.database import engine

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

# ─────────────────────────────────────────────
# 📊 RESUMEN GENERAL
# ─────────────────────────────────────────────
@router.get("/resumen")
def resumen():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_hechos,
                SUM(CASE WHEN Hecho LIKE '%CAPTURA%' THEN 1 ELSE 0 END) as capturas,
                SUM(CASE WHEN Hubo_Resultados = 'Si' THEN 1 ELSE 0 END) as con_resultado,
                SUM(COALESCE(Cantidad,0)) as total_afectados
            FROM datos
        """)).mappings().first()

    return result


# ─────────────────────────────────────────────
# 📈 TENDENCIA
# ─────────────────────────────────────────────
@router.get("/tendencia")
def tendencia():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                strftime('%Y-%m', Fechaing) as periodo,
                COUNT(*) as total
            FROM datos
            GROUP BY periodo
            ORDER BY periodo
        """)).mappings().all()

    return result


# ─────────────────────────────────────────────
# 🗺️ GEOGRAFÍA
# ─────────────────────────────────────────────
@router.get("/geografia")
def geografia():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                Dprtmnto,
                COUNT(*) as total
            FROM datos
            GROUP BY Dprtmnto
            ORDER BY total DESC
            LIMIT 10
        """)).mappings().all()

    return result


# ─────────────────────────────────────────────
# ⚔️ OPERACIONES
# ─────────────────────────────────────────────
@router.get("/operaciones")
def operaciones():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                Hecho,
                COUNT(*) as total
            FROM datos
            GROUP BY Hecho
            ORDER BY total DESC
            LIMIT 10
        """)).mappings().all()

    return result


# ─────────────────────────────────────────────
# 🎯 ENEMIGO
# ─────────────────────────────────────────────
@router.get("/enemigo")
def enemigo():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                Enemigo,
                COUNT(*) as total
            FROM datos
            GROUP BY Enemigo
            ORDER BY total DESC
        """)).mappings().all()

    return result