from fastapi import APIRouter
from sqlalchemy import text
from backend.database.database import engine

from fastapi import APIRouter, Depends
from sqlalchemy import text
from typing import Optional

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

# routes/dashboard.py


# ── Parámetros de filtro compartidos ────────────────────────────────────────────
def filtros_comunes(
    hecho:        Optional[str] = None,
    accion:       Optional[str] = None,
    tipo:         Optional[str] = None,
    subtipo:      Optional[str] = None,
    departamento: Optional[str] = None,
    municipio:    Optional[str] = None,
    year:         Optional[str] = None,
    month:        Optional[str] = None,
):
    return {
        "hecho": hecho, "accion": accion, "tipo": tipo, "subtipo": subtipo,
        "departamento": departamento, "municipio": municipio,
        "year": year, "month": month,
    }

def construir_where(filtros: dict) -> tuple[str, dict]:
    """Devuelve (cláusula WHERE, params) según los filtros activos."""
    clausulas = []
    params    = {}

    if filtros.get("hecho"):
        clausulas.append('Hecho = :hecho')
        params["hecho"] = filtros["hecho"]
    if filtros.get("accion"):
        clausulas.append('"Accion" = :accion')
        params["accion"] = filtros["accion"]
    if filtros.get("tipo"):
        clausulas.append('"Tipo" = :tipo')
        params["tipo"] = filtros["tipo"]
    if filtros.get("subtipo"):
        clausulas.append('"Subtipo" = :subtipo')
        params["subtipo"] = filtros["subtipo"]
    if filtros.get("departamento"):
        clausulas.append('"Dprtmnto" = :departamento')
        params["departamento"] = filtros["departamento"]
    if filtros.get("municipio"):
        clausulas.append('"Municipio" = :municipio')
        params["municipio"] = filtros["municipio"]
    if filtros.get("year"):
        clausulas.append("strftime('%Y', \"Fecha Hecho\") = :year")
        params["year"] = filtros["year"]
    if filtros.get("month"):
        clausulas.append("strftime('%m', \"Fecha Hecho\") = :month")
        params["month"] = filtros["month"]

    where = ("WHERE " + " AND ".join(clausulas)) if clausulas else ""
    return where, params


# ── /dashboard/kpis ─────────────────────────────────────────────────────────────
@router.get("/dashboard/kpis")
def kpis(filtros: dict = Depends(filtros_comunes)):
    where, params = construir_where(filtros)
    sql = f"""
        SELECT
            COUNT(*)            AS total_hechos,
            SUM(Cantidad)       AS total_cantidad,
            COUNT(DISTINCT Dprtmnto)  AS departamentos,
            COUNT(DISTINCT Municipio) AS municipios
        FROM datos
        {where}
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).mappings().one()
    return {
        "total_hechos":  row["total_hechos"]  or 0,
        "total_cantidad": row["total_cantidad"] or 0,
        "departamentos": row["departamentos"] or 0,
        "municipios":    row["municipios"]    or 0,
    }


# ── /dashboard/agrupacion ────────────────────────────────────────────────────────
CAMPOS_PERMITIDOS = {
    "hecho":        "Hecho",
    "accion":       "Accion",
    "tipo":         "Tipo",
    "subtipo":      "Subtipo",
    "departamento": "Dprtmnto",
    "municipio":    "Municipio",
    "clase":        "Clase",
    "enemigo":      "Enemigo",
    "year":         "strftime('%Y', \"Fecha Hecho\")",
    "month":        "strftime('%m', \"Fecha Hecho\")",
}

@router.get("/dashboard/agrupacion")
def agrupacion(
    campo:  str,
    limite: int = 200,
    filtros: dict = Depends(filtros_comunes),
):
    if campo not in CAMPOS_PERMITIDOS:
        return {"error": "campo no permitido", "data": []}

    col   = CAMPOS_PERMITIDOS[campo]
    where, params = construir_where(filtros)

    sql = f"""
        SELECT {col} AS label, COUNT(*) AS total
        FROM datos
        {where}
        GROUP BY {col}
        ORDER BY total DESC
        LIMIT :limite
    """
    params["limite"] = limite

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return {
        "campo": campo,
        "data": [{"label": r["label"] or "—", "total": r["total"]} for r in rows],
    }


# ── /dashboard/registros ─────────────────────────────────────────────────────────
@router.get("/dashboard/registros")
def registros(
    page:  int = 1,
    limit: int = 50,
    filtros: dict = Depends(filtros_comunes),
):
    where, params = construir_where(filtros)
    offset = (page - 1) * limit

    sql_total = f"SELECT COUNT(*) FROM datos {where}"
    sql_data  = f"""
        SELECT
            "Fecha Hecho", Hecho, Accion, Tipo, Subtipo,
            Dprtmnto, Municipio, Clase, Enemigo, Cantidad
        FROM datos
        {where}
        ORDER BY "Fecha Hecho" DESC
        LIMIT :limit OFFSET :offset
    """
    params_data = {**params, "limit": limit, "offset": offset}

    with engine.connect() as conn:
        total = conn.execute(text(sql_total), params).scalar() or 0
        rows  = conn.execute(text(sql_data), params_data).mappings().all()

    return {
        "page":        page,
        "limit":       limit,
        "total":       total,
        "total_pages": (total + limit - 1) // limit,
        "data": [dict(r) for r in rows],
    }


# ── /dashboard/filtros-disponibles ──────────────────────────────────────────────
# Años y meses existentes en la BD (sin filtros, para poblar los selects)
@router.get("/dashboard/filtros-disponibles")
def filtros_disponibles():
    sql = """
        SELECT
            strftime('%Y', "Fecha Hecho") AS year,
            strftime('%m', "Fecha Hecho") AS month
        FROM datos
        WHERE "Fecha Hecho" IS NOT NULL
        GROUP BY year, month
        ORDER BY year DESC, month ASC
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()

    years  = sorted({r["year"]  for r in rows if r["year"]},  reverse=True)
    months = sorted({r["month"] for r in rows if r["month"]})
    return {"years": years, "months": months}