from sqlalchemy import create_engine
import os

# Detecta si estás en Render
IS_RENDER = os.getenv("RENDER") is not None

if IS_RENDER:
    # 🔥 Ruta segura en Render
    db_path = "/tmp/test.db"
else:
    # 💻 Ruta local (tu lógica original)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(BASE_DIR, "data", "test.db")

DATABASE_URL = f"sqlite:///{db_path}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)