from fastapi import APIRouter, Depends
from sqlalchemy import text
from backend.database.database import engine
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
                TO_CHAR(Fechaing, 'YYYY-MM') as periodo,
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


# ─────────────────────────────────────────────
# 🧠 FILTROS BASE
# ─────────────────────────────────────────────
def filtros_comunes(
    hecho: Optional[str] = None,
    accion: Optional[str] = None,
    tipo: Optional[str] = None,
    subtipo: Optional[str] = None,
    departamento: Optional[str] = None,
    municipio: Optional[str] = None,
    year: Optional[str] = None,
    month: Optional[str] = None,
):
    return locals()


def construir_where(filtros: dict):
    clausulas = []
    params = {}

    mapa = {
        "hecho": "Hecho",
        "accion": "Accion",
        "tipo": "Tipo",
        "subtipo": "Subtipo",
        "departamento": "Dprtmnto",
        "municipio": "Municipio",
    }

    for k, col in mapa.items():
        if filtros.get(k):
            clausulas.append(f'"{col}" = :{k}')
            params[k] = filtros[k]

    if filtros.get("year"):
        clausulas.append("EXTRACT(YEAR FROM \"Fecha Hecho\") = :year")
        params["year"] = int(filtros["year"])  # Convert to int for comparison

    if filtros.get("month"):
        clausulas.append("EXTRACT(MONTH FROM \"Fecha Hecho\") = :month")
        params["month"] = int(filtros["month"])

    where = "WHERE " + " AND ".join(clausulas) if clausulas else ""
    return where, params


# ─────────────────────────────────────────────
# 📊 KPIS
# ─────────────────────────────────────────────
@router.get("/kpis")
def kpis(filtros: dict = Depends(filtros_comunes)):

    where, params = construir_where(filtros)

    sql = f"""
    SELECT
        COUNT(*) as total_hechos,
        SUM(COALESCE(Cantidad,0)) as total_cantidad,
        COUNT(DISTINCT Dprtmnto) as departamentos,
        COUNT(DISTINCT Municipio) as municipios
    FROM datos
    {where}
    """

    with engine.connect() as conn:
        row = conn.execute(text(sql), params).mappings().one()

    return {
        "total_hechos": row["total_hechos"] or 0,
        "total_cantidad": row["total_cantidad"] or 0,
        "departamentos": row["departamentos"] or 0,
        "municipios": row["municipios"] or 0,
    }


# ─────────────────────────────────────────────
# 📊 AGRUPACIÓN
# ─────────────────────────────────────────────
CAMPOS_PERMITIDOS = {
    "hecho": "Hecho",
    "accion": "Accion",
    "tipo": "Tipo",
    "subtipo": "Subtipo",
    "departamento": "Dprtmnto",
    "municipio": "Municipio",
}

@router.get("/agrupacion")
def agrupacion(
    campo: str,
    filtros: dict = Depends(filtros_comunes),
):

    if campo not in CAMPOS_PERMITIDOS:
        return {"data": []}

    col = CAMPOS_PERMITIDOS[campo]
    where, params = construir_where(filtros)

    sql = f"""
    SELECT {col} as label, COUNT(*) as total
    FROM datos
    {where}
    GROUP BY {col}
    ORDER BY total DESC
    LIMIT 50
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return {"data": rows}


# ─────────────────────────────────────────────
# 📋 REGISTROS (TABLA)
# ─────────────────────────────────────────────
@router.get("/registros")
def registros(
    page: int = 1,
    limit: int = 50,
    filtros: dict = Depends(filtros_comunes),
):

    where, params = construir_where(filtros)
    offset = (page - 1) * limit

    sql_total = f"SELECT COUNT(*) FROM datos {where}"

    sql_data = f"""
    SELECT
        "Fecha Hecho",
        Hecho, Accion, Tipo, Subtipo,
        Dprtmnto, Municipio,
        Enemigo, Cantidad
    FROM datos
    {where}
    ORDER BY "Fecha Hecho" DESC
    LIMIT :limit OFFSET :offset
    """

    with engine.connect() as conn:
        total = conn.execute(text(sql_total), params).scalar()
        rows = conn.execute(
            text(sql_data),
            {**params, "limit": limit, "offset": offset}
        ).mappings().all()

    return {
        "page": page,
        "total": total,
        "total_pages": (total + limit - 1) // limit,
        "data": rows
    }