FROM python:3.11

WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el proyecto
COPY . .

# Crear carpeta uploads (por si no existe)
RUN mkdir -p uploads

# Exponer puerto
EXPOSE 8000

# Ejecutar API
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]