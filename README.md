# Proyecto Integrador de Big Data - Documentación.

## Descripción del proyecto
Este proyecto implementa un pipeline completo de procesamiento de datos en un entorno de Big Data, dividido en tres etapas principales:

Ingestión de datos desde una API y almacenamiento en SQLite

Preprocesamiento y limpieza de datos en un entorno simulado en la nube

Enriquecimiento del conjunto de datos con fuentes adicionales

# Estructura del proyecto
El proyecto se divide en tres módulos principales:
- **ingestion_datos**: Módulo que se encarga de la ingestión de datos desde una API y su almacenamiento en una base de datos SQLite.

- **preprocesamiento**: Módulo que se encarga del preprocesamiento y limpieza de los datos en un entorno simulado en la nube
- **Enriquecimiento de datos**: Módulo que se encarga de enriquecer el conjunto de datos con fuentes adicionales.
# Tecnologías utilizadas
- **Python 3.9**: Lenguaje de programación utilizado para el desarrollo del proyecto.


- **SQLite**: Base de datos utilizada para almacenar los datos.

- **pandas**: Biblioteca utilizada para el manejo y análisis de datos.

- **numpy**: Biblioteca utilizada para el manejo de matrices y vectores numéricos.

- **git**: Herramienta de control de versiones utilizada para el seguimiento del código.
- **git action**: Herramienta utilizada para automatizar la ejecución de tareas y la integración continua.  

- **Simulación de entorno en la nube**: Utilizado para el preproces
amiento y limpieza de los datos.
- **API**: Utilizada para la ingestión de datos.


- **Fuentes adicionales**: Utilizadas para enriquecer el conjunto de datos.

# Requisitos previos

- **Python 3.9**: Instalado en el entorno de desarrollo.
- **SQLite**: Instalado en el entorno de desarrollo.


- **Simulación de entorno en la nube**: Instalado en el entorno de desarrollo.


- **API**: Conectada y accesible.

- **Fuentes adicionales**: Disponibles y accesibles.

# Instalación
Para instalar el proyecto, seguir los siguientes pasos:
1. Clonar el repositorio utilizando el comando `git clone`.
2. Crear entorno virtual utilizando el comando `python -m venv venv`.
3. Instalar las dependencias utilizando el comando `pip install -r requirements.txt`.

# Ejecución del proyecto
Para ejecutar el proyecto, seguir los siguientes pasos:
1. Activar el entorno virtual utilizando el comando `source venv/bin/activate`.

**1. Ingestión de datos**

2. Ejecutar el ingestion.py utilizando el comando `python src/ingestion.py`.
3. Tenemos la siguiente salida : 

**Salidas**
- Base de datos SQLite: **src/static/db/ingestion.db**

- Muestra de datos: **src/static/xlsx/ingestion.xlsx**

- Reporte de auditoría: **src/static/auditoria/ingestion.txt**

**2. Preprocesamiento y limpieza de datos**:
- Ejecutar el preprocesamiento.py utilizando el comando `python src/cleaning.py`

**Salidas:**

Datos limpios: **src/static/xlsx/cleaned_data.xlsx**

Reporte de limpieza: **src/static/auditoria/cleaning_report.txt**

**3. Enriquecimiento de Datos**
- Ejecutar el enriquecimiento.py utilizando el comando `python src/enrichment.py`

**Salidas:**

Datos enriquecidos: **src/static/xlsx/enriched_data.xlsx**

Reporte de enriquecimiento: **src/static/auditoria/enriched_report.txt**

# Estructura del proyecto
El proyecto se estructura en las siguientes carpetas:

```.
├── data_cleaning.log
├── data.json
├── docs
│   ├── arquitectura_app.drawio
│   ├── diagrama_datos.drawio
│   └── modelo_datos.drawio
├── enrichment.log
├── proyecto_big_data.egg-info
│   ├── dependency_links.txt
│   ├── PKG-INFO
│   ├── requires.txt
│   ├── SOURCES.txt
│   └── top_level.txt
├── proyectoIudigital-env
│   ├── bin
│   │   ├── activate
│   │   ├── activate.csh
│   │   ├── activate.fish
│   │   ├── Activate.ps1
│   │   ├── f2py
│   │   ├── fonttools
│   │   ├── normalizer
│   │   ├── numpy-config
│   │   ├── pip
│   │   ├── pip3
│   │   ├── pip3.12
│   │   ├── pyftmerge
│   │   ├── pyftsubset
│   │   ├── python -> python3
│   │   ├── python3 -> /usr/bin/python3
│   │   ├── python3.12 -> python3
│   │   └── ttx
│   ├── include
│   │   └── python3.12
│   ├── lib
│   │   └── python3.12
│   ├── lib64 -> lib
│   ├── pyvenv.cfg
│   └── share
│       └── man
├── README.md
├── requirements.txt
├── script.py
├── setup.py
└── src
    ├── cleaning.py
    ├── enrichement.py
    ├── ingesta.py
    └── static
        ├── auditoria
        ├── cleaned_data
        ├── data_sources
        ├── db
        ├── enriched_data
        └── xlsx
```

***Automatización con GitHub Actions***
=====================================
### Crear un archivo de configuración para GitHub Actions 
Para automatizar el proceso de construcción y publicación de nuestro proyecto, necesitamos crear un archivo de configuración para GitHub Actions. Este archivo se llama `.github/workflows/test.yml`. El orden de ejecución es el siguiente: 

- ingesta de datos desde la API

- proceso de limpieza

- enriquecimiento del dataset

```
name: Actividad Ingesta de Datos

on:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: windows-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.9.2'

      - name: Crear entorno virtual
        run: python -m venv venv

      - name: Instalar dependencias
        run: |
          venv\Scripts\python -m pip install --upgrade pip
          venv\Scripts\python -m pip install -r requirements.txt || echo "requirements.txt no encontrado, instalando paquetes manualmente..."
          venv\Scripts\python -m pip install pandas requests openpyxl  # Se agrega openpyxl

      - name: Verificar instalación de openpyxl
        run: venv\Scripts\python -c "import openpyxl; print('openpyxl instalado correctamente')"

      - name: Ejecutar script de ingesta
        run: venv\Scripts\python src/ingesta.py

      - name: Ejecutar limpieza de datos
        run: venv\Scripts\python src/cleaning.py
      
      - name: Ejecutar enriquecimiento de datos
        run: venv\Scripts\python src/enrichement.py

      - name: Commit and Push changes
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: generación informe prueba json
          commit_user_name: Omar Alvarez [GitHub Actions]
          commit_user_email: omaraleyser@hotmail.com
          commit_author: Omar Alvarez <omaraleyser@hotmail.com>

```








