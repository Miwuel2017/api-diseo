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

@router.get("/bloque-operacional")
def bloque_operacional():

    with engine.connect() as conn:

        result = conn.execute(text("""
            SELECT 
                Boletin,
                Hecho,
                Accion,
                Tipo,
                Subtipo,
                "Fecha Hecho",
                Mes,
                Radiograma,
                "Ampliación",
                "Detalles Del Lugar",
                "Ambiente Operacional",
                Municipio,
                Dprtmnto,
                Division,
                Brigada,
                Batallon,
                Enemigo,
                Subestructura,
                "Ord. Batallón",
                "Acción Enemiga",
                "Obs. Detal. Opera.",
                Clase,
                Cantidad,
                "Unidad Med",
                Complemento1,
                Identificacion,
                Edad,
                Sexo,
                "Tpo Organizacion",
                Judicializado
            FROM datos
            LIMIT 500
        """)).mappings().all()

    # 🔥 AQUÍ VIENE LO IMPORTANTE → ENCAPSULADO
    estructura = {}

    for row in result:

        b = row["Boletin"]
        h = row["Hecho"]
        a = row["Accion"]
        t = row["Tipo"]
        s = row["Subtipo"]

        estructura.setdefault(b, {})
        estructura[b].setdefault(h, {})
        estructura[b][h].setdefault(a, {})
        estructura[b][h][a].setdefault(t, {})
        estructura[b][h][a][t].setdefault(s, [])

        estructura[b][h][a][t][s].append(dict(row))

    return estructura