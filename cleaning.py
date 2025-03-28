import pandas as pd
import sqlite3
import logging
from pathlib import Path
import os

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_cleaning.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Configuración de rutas
BASE_DIR = Path(os.getcwd())  # Usa el directorio actual en lugar de __file__
DB_PATH = BASE_DIR / "src" / "static" / "db" / "ingestion.db"
CLEANED_DATA_PATH = BASE_DIR / "src" / "static" / "cleaned_data" / "cleaned_data.xlsx"
AUDIT_PATH = BASE_DIR / "src" / "static" / "auditoria" / "cleaning_report.txt"

# Asegurar que los directorios existan
for path in [DB_PATH.parent, CLEANED_DATA_PATH.parent, AUDIT_PATH.parent]:
    os.makedirs(path, exist_ok=True)

def load_data():
    """Carga datos desde la base de datos SQLite"""
    try:
        logger.info(f"Cargando datos desde: {DB_PATH}")

        # Verificar si la base de datos existe
        if not DB_PATH.exists():
            raise FileNotFoundError(f"La base de datos no existe en {DB_PATH}")

        # Conectar a la base de datos
        with sqlite3.connect(str(DB_PATH)) as conn:
            query = "SELECT * FROM jobs"
            df = pd.read_sql_query(query, conn)

        logger.info(f"Datos cargados correctamente. Registros: {len(df)}")
        return df

    except Exception as e:
        logger.exception("Error al cargar datos")
        raise

def analyze_data(df):
    """Realiza análisis exploratorio de los datos"""
    analysis = {
        "initial_records": len(df),
        "null_counts": df.isnull().sum().to_dict(),
        "duplicates": df.duplicated().sum(),
        "data_types": df.dtypes.to_dict(),
    }

    logger.info("Análisis exploratorio completado")
    return analysis

def clean_data(df):
    """Realiza la limpieza y transformación de datos"""
    df_clean = df.copy()
    cleaning_report = {"duplicates_removed": 0, "null_handling": {}, "type_conversions": {}, "transformations": {}}

    # 1. Eliminar duplicados
    initial_count = len(df_clean)
    df_clean.drop_duplicates(inplace=True)
    cleaning_report["duplicates_removed"] = initial_count - len(df_clean)

    # 2. Manejo de valores nulos
    for col in df_clean.columns:
        null_count = df_clean[col].isnull().sum()
        if null_count > 0:
            if pd.api.types.is_numeric_dtype(df_clean[col]):
                fill_value = df_clean[col].median()
            else:
                mode_values = df_clean[col].mode()
                fill_value = mode_values[0] if not mode_values.empty else ""

            df_clean[col].fillna(fill_value, inplace=True)
            cleaning_report["null_handling"][col] = f"Rellenados {null_count} nulos con {fill_value}"

    # 3. Corrección de tipos de datos
    if "remote" in df_clean.columns:
        df_clean["remote"] = df_clean["remote"].astype(bool)
        cleaning_report["type_conversions"]["remote"] = "Convertido a booleano"

    # 4. Transformaciones adicionales
    if "company_name" in df_clean.columns:
        df_clean["company_name"] = df_clean["company_name"].str.title()
        cleaning_report["transformations"]["company_name"] = "Normalizado a título case"

    logger.info("Limpieza de datos completada")
    return df_clean, cleaning_report

def generate_artifacts(clean_df, analysis, cleaning_report):
    """Genera los archivos de salida"""
    try:
        # Guardar datos limpios
        clean_df.to_excel(CLEANED_DATA_PATH, index=False)
        logger.info(f"Datos limpios guardados en: {CLEANED_DATA_PATH}")

        # Generar reporte de auditoría
        report_content = [
            "=== REPORTE DE LIMPIEZA DE DATOS ===",
            f"Fecha: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "=== ESTADO INICIAL ===",
            f"Total de registros: {analysis['initial_records']}",
            f"Registros duplicados: {analysis['duplicates']}",
            "Valores nulos por columna:",
        ]

        for col, count in analysis["null_counts"].items():
            report_content.append(f"- {col}: {count}")

        report_content.extend([
            "",
            "=== OPERACIONES REALIZADAS ===",
            f"Registros duplicados eliminados: {cleaning_report['duplicates_removed']}",
            "Manejo de valores nulos:",
        ])

        for col, msg in cleaning_report["null_handling"].items():
            report_content.append(f"- {col}: {msg}")

        report_content.append("\n=== ESTADO FINAL ===")
        report_content.append(f"Total de registros: {len(clean_df)}")
        report_content.append("Valores nulos por columna:")

        for col, count in clean_df.isnull().sum().items():
            report_content.append(f"- {col}: {count}")

        # Escribir reporte
        with open(AUDIT_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(report_content))

        logger.info(f"Reporte de auditoría generado en: {AUDIT_PATH}")

    except Exception as e:
        logger.exception("Error al generar artefactos")
        raise

def main():
    try:
        logger.info("Iniciando proceso de limpieza de datos")

        # 1. Cargar datos
        df = load_data()

        # 2. Análisis inicial
        initial_analysis = analyze_data(df)

        # 3. Limpieza de datos
        clean_df, cleaning_report = clean_data(df)

        # 4. Generar artefactos
        generate_artifacts(clean_df, initial_analysis, cleaning_report)

        logger.info("Proceso de limpieza completado exitosamente")
        return 0

    except Exception as e:
        logger.exception("Error en el proceso de limpieza")
        return 1

if __name__ == "__main__":
    # Debug: mostrar rutas importantes
    logger.info(f"Directorio base: {BASE_DIR}")
    logger.info(f"Ruta de la base de datos: {DB_PATH}")
    logger.info(f"Ruta de salida de datos limpios: {CLEANED_DATA_PATH}")
    logger.info(f"Ruta del reporte de auditoría: {AUDIT_PATH}")

    exit(main())