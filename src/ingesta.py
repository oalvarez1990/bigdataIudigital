import requests
import sqlite3
import pandas as pd
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "static" / "db" / "ingestion.db"
AUDIT_PATH = BASE_DIR / "static" / "auditoria" / "ingestion.txt"
SAMPLE_PATH = BASE_DIR / "static" / "xlsx" / "ingestion.xlsx"

# Configuración de la API de Arbeitnow
ARBEITNOW_API_URL = "https://www.arbeitnow.com/api/job-board-api"

# 1. Extraer datos del API de Arbeitnow
def fetch_data_from_api():
    response = requests.get(ARBEITNOW_API_URL)
    if response.status_code == 200:
        return response.json()["data"]
    else:
        raise Exception(f"Error al conectar al API: {response.status_code}")

# 2. Crear y almacenar datos en SQLite
def create_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            company_name TEXT NOT NULL,
            location TEXT NOT NULL,
            remote BOOLEAN NOT NULL,
            url TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def insert_data_into_db(data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for job in data:
        cursor.execute("""
            INSERT INTO jobs (title, company_name, location, remote, url)
            VALUES (?, ?, ?, ?, ?)
        """, (job["title"], job["company_name"], job["location"], job["remote"], job["url"]))
    conn.commit()
    conn.close()

# 3. Generar archivo de muestra con Pandas
def generate_sample_file():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM jobs LIMIT 10", conn)  # Muestra de 10 registros
    df.to_excel(SAMPLE_PATH, index=False)
    conn.close()

# 4. Generar archivo de auditoría
def generate_audit_file(api_data):
    conn = sqlite3.connect(DB_PATH)
    db_data = pd.read_sql_query("SELECT * FROM jobs", conn)
    conn.close()

    with open(AUDIT_PATH, "w") as file:
        file.write(f"Registros extraídos del API: {len(api_data)}\n")
        file.write(f"Registros almacenados en la base de datos: {len(db_data)}\n")
        file.write("Comparación de integridad: OK\n" if len(api_data) == len(db_data) else "Comparación de integridad: ERROR\n")

# Ejecución principal
if __name__ == "__main__":
    # Extraer datos del API
    api_data = fetch_data_from_api()

    # Crear base de datos e insertar datos
    create_database()
    insert_data_into_db(api_data)

    # Generar archivos de evidencia
    generate_sample_file()
    generate_audit_file(api_data)

    print("Proceso de ingesta completado con éxito.")