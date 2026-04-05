from sqlalchemy import create_engine
import os

DATABASE_URL = os.getenv("postgresql://sicoe_user:yVZkQp0BpyN3atmsEEPmmzNm5ERHNLBc@dpg-d79cmk2a214c73api600-a/sicoe")

if not DATABASE_URL:
    # Fallback para desarrollo local con SQLite
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATABASE_URL = f"sqlite:///{os.path.join(BASE_DIR, 'data', 'test.db')}"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)