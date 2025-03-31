import pandas as pd
import json
import xml.etree.ElementTree as ET
from pathlib import Path
import logging
from datetime import datetime
import os

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("enrichment.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuración de rutas
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_SOURCES_DIR = BASE_DIR / "src" / "static" / "data_sources"
CLEANED_DATA_PATH = BASE_DIR / "src" / "static" / \
    "cleaned_data" / "cleaned_data.xlsx"
ENRICHED_DATA_PATH = BASE_DIR / "src" / "static" / \
    "enriched_data" / "enriched_data.xlsx"
AUDIT_PATH = BASE_DIR / "src" / "static" / "auditoria" / "enriched_report.txt"

# Asegurar que los directorios existan
os.makedirs(ENRICHED_DATA_PATH.parent, exist_ok=True)
os.makedirs(AUDIT_PATH.parent, exist_ok=True)


def load_clean_data():
    """Carga los datos limpios de la actividad anterior"""
    try:
        logger.info(f"Cargando datos limpios desde: {CLEANED_DATA_PATH}")
        df = pd.read_excel(CLEANED_DATA_PATH)
        logger.info(f"Datos limpios cargados. Registros: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al cargar datos limpios: {str(e)}")
        raise


def load_json_data():
    """Carga datos adicionales desde archivo JSON"""
    json_path = DATA_SOURCES_DIR / "companies_info.json"
    try:
        logger.info(f"Cargando datos JSON desde: {json_path}")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        logger.info(f"Datos JSON cargados. Registros: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al cargar JSON: {str(e)}")
        raise


def load_csv_data():
    """Carga datos adicionales desde archivo CSV"""
    csv_path = DATA_SOURCES_DIR / "salary_ranges.csv"
    try:
        logger.info(f"Cargando datos CSV desde: {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info(f"Datos CSV cargados. Registros: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al cargar CSV: {str(e)}")
        raise


def load_excel_data():
    """Carga datos adicionales desde archivo Excel"""
    excel_path = DATA_SOURCES_DIR / "locations.xlsx"
    try:
        logger.info(f"Cargando datos Excel desde: {excel_path}")
        
        # Verificación exhaustiva del archivo
        if not excel_path.exists():
            available_files = [f.name for f in DATA_SOURCES_DIR.glob('*') if f.is_file()]
            raise FileNotFoundError(
                f"El archivo {excel_path.name} no existe. "
                f"Archivos disponibles: {available_files}"
            )
        
        if os.path.getsize(excel_path) == 0:
            raise ValueError("El archivo Excel está vacío")
        
        # Intentar con diferentes motores como fallback
        try:
            df = pd.read_excel(excel_path, engine='openpyxl')
        except Exception as e:
            logger.warning(f"Error con openpyxl, intentando con xlrd: {str(e)}")
            try:
                df = pd.read_excel(excel_path, engine='xlrd')
            except Exception as e:
                logger.error("Ambos motores fallaron")
                raise
        
        if df.empty:
            raise ValueError("El DataFrame cargado está vacío")
            
        logger.info(f"Datos Excel cargados. Registros: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"Error al cargar Excel: {str(e)}")
        logger.info("Asegúrate de que:")
        logger.info("1. El archivo existe y no está vacío")
        logger.info("2. Tienes instalado openpyxl: pip install openpyxl")
        logger.info("3. El archivo es un Excel válido (puedes abrirlo con Excel/LibreOffice)")
        raise


def load_xml_data():
    """Carga datos adicionales desde archivo XML"""
    xml_path = DATA_SOURCES_DIR / "industry_data.xml"
    try:
        logger.info(f"Cargando datos XML desde: {xml_path}")

        tree = ET.parse(xml_path)
        root = tree.getroot()

        data = []
        for item in root.findall('industry'):
            record = {
                'industry_id': item.find('id').text,
                'industry_name': item.find('name').text,
                'growth_rate': float(item.find('growth_rate').text),
                'avg_salary': float(item.find('avg_salary').text)
            }
            data.append(record)

        df = pd.DataFrame(data)
        logger.info(f"Datos XML cargados. Registros: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al cargar XML: {str(e)}")
        raise


def enrich_data(base_df):
    """Realiza el proceso de enriquecimiento con todas las fuentes"""
    enrichment_report = {
        'base_records': len(base_df),
        'sources': {},
        'operations': []
    }

    try:
        # 1. Enriquecer con información de empresas (JSON)
        companies_df = load_json_data()
        enriched_df = pd.merge(
            base_df,
            companies_df,
            left_on='company_name',
            right_on='name',
            how='left'
        )
        matches = len(enriched_df[~enriched_df['name'].isna()])
        enrichment_report['sources']['companies_info.json'] = {
            'matched_records': matches,
            'new_columns': list(companies_df.columns.difference(base_df.columns))
        }
        enrichment_report['operations'].append(
            f"Merge con companies_info.json: {matches} registros coincidentes"
        )

        # 2. Enriquecer con rangos salariales (CSV)
        salary_df = load_csv_data()
        enriched_df = pd.merge(
            enriched_df,
            salary_df,
            left_on='title',
            right_on='job_title',
            how='left'
        )
        matches = len(enriched_df[~enriched_df['job_title'].isna()])
        enrichment_report['sources']['salary_ranges.csv'] = {
            'matched_records': matches,
            'new_columns': list(salary_df.columns.difference(enriched_df.columns))
        }
        enrichment_report['operations'].append(
            f"Merge con salary_ranges.csv: {matches} registros coincidentes"
        )

        # 3. Enriquecer con ubicaciones (Excel)
        locations_df = load_excel_data()
        enriched_df = enriched_df.merge(
            locations_df,
            left_on='location',
            right_on='city',
            how='left'
        )
        matches = len(enriched_df[~enriched_df['city'].isna()])
        enrichment_report['sources']['locations.xlsx'] = {
            'matched_records': matches,
            'new_columns': list(locations_df.columns.difference(enriched_df.columns))
        }
        enrichment_report['operations'].append(
            f"Merge con locations.xlsx: {matches} registros coincidentes"
        )

        # 4. Enriquecer con datos de industria (XML)
        industry_df = load_xml_data()
        enriched_df = enriched_df.merge(
            industry_df,
            left_on='industry',
            right_on='industry_id',
            how='left'
        )
        matches = len(enriched_df[~enriched_df['industry_id'].isna()])
        enrichment_report['sources']['industry_data.xml'] = {
            'matched_records': matches,
            'new_columns': list(industry_df.columns.difference(enriched_df.columns))
        }
        enrichment_report['operations'].append(
            f"Merge con industry_data.xml: {matches} registros coincidentes"
        )

        # Limpieza post-enriquecimiento
        enriched_df.drop(columns=['name', 'job_title', 'city',
                         'industry_id'], inplace=True, errors='ignore')

        enrichment_report['final_records'] = len(enriched_df)
        enrichment_report['new_columns_total'] = len(
            enriched_df.columns) - len(base_df.columns)

        logger.info("Proceso de enriquecimiento completado")
        return enriched_df, enrichment_report

    except Exception as e:
        logger.error(f"Error durante el enriquecimiento: {str(e)}")
        raise


def generate_artifacts(enriched_df, report):
    """Genera los archivos de salida"""
    try:
        # Guardar datos enriquecidos
        enriched_df.to_excel(ENRICHED_DATA_PATH, index=False)
        logger.info(f"Datos enriquecidos guardados en: {ENRICHED_DATA_PATH}")

        # Generar reporte de auditoría
        report_content = [
            "=== REPORTE DE ENRIQUECIMIENTO DE DATOS ===",
            f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Dataset base: {CLEANED_DATA_PATH}",
            f"Registros iniciales: {report['base_records']}",
            f"Registros finales: {report['final_records']}",
            f"Columnas añadidas: {report['new_columns_total']}",
            "",
            "=== FUENTES DE DATOS ==="
        ]

        for source, info in report['sources'].items():
            report_content.extend([
                f"Fuente: {source}",
                f"- Registros coincidentes: {info['matched_records']}",
                f"- Columnas añadidas: {', '.join(info['new_columns'])}",
                ""
            ])

        report_content.extend([
            "=== OPERACIONES REALIZADAS ==="
        ] + report['operations'] + [
            "",
            "=== RESUMEN FINAL ===",
            f"Total de registros: {report['final_records']}",
            f"Total de columnas: {len(enriched_df.columns)}",
            f"Columnas originales: {len(enriched_df.columns) - report['new_columns_total']}",
            f"Columnas añadidas: {report['new_columns_total']}"
        ])

        with open(AUDIT_PATH, 'w', encoding='utf-8') as f:
            f.write("\n".join(report_content))

        logger.info(f"Reporte de auditoría generado en: {AUDIT_PATH}")

    except Exception as e:
        logger.error(f"Error al generar artefactos: {str(e)}")
        raise


def main():
    try:
        logger.info("Iniciando proceso de enriquecimiento de datos")

        # 1. Cargar datos base
        base_df = load_clean_data()

        # 2. Enriquecer datos
        enriched_df, enrichment_report = enrich_data(base_df)

        # 3. Generar artefactos
        generate_artifacts(enriched_df, enrichment_report)

        logger.info("Proceso de enriquecimiento completado exitosamente")
        return 0

    except Exception as e:
        logger.error(f"Error en el proceso: {str(e)}")
        return 1


if __name__ == "__main__":
    # Debug: mostrar información de rutas
    logger.info(f"Directorio base: {BASE_DIR}")
    logger.info(f"Directorio de fuentes: {DATA_SOURCES_DIR}")
    logger.info(f"Ruta de datos limpios: {CLEANED_DATA_PATH}")
    logger.info(f"Ruta de salida enriquecida: {ENRICHED_DATA_PATH}")

    exit_code = main()
    exit(exit_code)
