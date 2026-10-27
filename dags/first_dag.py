
"""
DAG: ETL diario de ventas – DataMart S.A.S.

Horario: todos los días a las 6:00 PM (UTC-5) → 23:00 UTC.
Pipeline: extract → transform → load.

- Usa decoradores @dag / @task (TaskFlow API – Airflow 3.x).
- La lógica de negocio vive en src/datamart/ (sin Airflow).
- Los archivos CSV originales están en data/raw/.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task, Variable

# ── Importaciones de lógica de negocio ────────────────────────────────────
from datamart import (
    extraer_csv,
    transform_sources,
    validar_y_separar,
    calcular_revenue_bruto,
    calcular_resumen_diario,
    obtener_engine,
    cargar_transacciones,
    cargar_rechazos,
    cargar_productos,
    cargar_resumen_diario,
)

# ── Rutas de archivos ────────────────────────────────────────────────────
DATA_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"
logger = logging.getLogger("airflow.task")

@dag(
    dag_id="datamart_etl_ventas",
    description=(
        "ETL diario: extracción, limpieza, validación y carga "
        "de ventas y devoluciones al Data Warehouse analítico"
    ),
    schedule="0 23 * * *",
    start_date=datetime(2026, 10, 25),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "luis-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["datamart", "etl", "ventas", "postgres"],
)
def datamart_etl_ventas():

    def extraer_y_guardar(nombre_archivo, **context):
        """Lee una fuente, registra su calidad básica y guarda Parquet."""
        carpeta_datos = Variable.get("RAW_CSV_PATH", DATA_DIR)
        directorio_procesado = Variable.get("PROCESSED_PATH", PROCESSED_DIR)
        run_id = context["run_id"].replace("/", "_")
        processed_dir = os.path.join(directorio_procesado, run_id)
        os.makedirs(processed_dir, exist_ok=True)

        dataframe = extraer_csv(
            ruta=os.path.join(carpeta_datos, nombre_archivo),
            nombre_fuente=nombre_archivo,
        )
        duplicados = dataframe.drop(
            columns=["source_file", "source_row_index"],
            errors="ignore",
        ).duplicated().sum()
        logger.info(
            "Perfil de %s: %d filas, %d columnas, %d nulos y %d "
            "duplicados exactos",
            nombre_archivo,
            len(dataframe),
            len(dataframe.columns),
            int(dataframe.isna().sum().sum()),
            int(duplicados),
        )
        logger.info(
            "Nulos por columna en %s: %s",
            nombre_archivo,
            dataframe.isna().sum().to_dict(),
        )
        logger.info(
            "Tipos de columna en %s: %s",
            nombre_archivo,
            dataframe.dtypes.astype(str).to_dict(),
        )
        for columna in ("Quantity", "UnitPrice"):
            if columna in dataframe:
                logger.info(
                    "Rango de %s en %s: %s a %s",
                    columna,
                    nombre_archivo,
                    dataframe[columna].min(),
                    dataframe[columna].max(),
                )
        if "Country" in dataframe:
            logger.info(
                "Países distintos en %s: %d",
                nombre_archivo,
                dataframe["Country"].nunique(),
            )

        ruta_salida = os.path.join(
            processed_dir,
            f"{Path(nombre_archivo).stem}_raw.parquet",
        )
        dataframe.to_parquet(ruta_salida, index=False)
        return ruta_salida

    # ── TASK 1: EXTRACCIÓN POR FUENTE ────────────────────────────────────
    @task
    def extract_sales(**context):
        """Extrae el CSV de ventas."""
        return extraer_y_guardar("data.csv", **context)

    @task
    def extract_history(**context):
        """Extrae el CSV histórico disponible."""
        return extraer_y_guardar("online_retail.csv", **context)

    # ── TASK 2: TRANSFORMACIÓN ────────────────────────────────────────────
    @task
    def transform(ruta_ventas, ruta_historico, **context):
        """Limpia, valida y prepara las transacciones."""

        import pandas as pd

        df_principal = pd.read_parquet(ruta_ventas)
        df_secundario = pd.read_parquet(ruta_historico)

        run_id = context["run_id"].replace("/", "_")
        directorio_procesado = Variable.get("PROCESSED_PATH", PROCESSED_DIR)
        processed_dir = os.path.join(directorio_procesado, run_id)
        os.makedirs(processed_dir, exist_ok=True)

        df = transform_sources(
            sales=df_principal,
            history=df_secundario,
        )
        productos = (
            df.dropna(subset=["StockCode"])
            .drop_duplicates(subset=["StockCode"])
        )
        mapa_productos = productos.set_index("StockCode")[
            "canonical_description"
        ].to_dict()
        categorias_productos = productos.set_index("StockCode")[
            "category"
        ].to_dict()

        # Separar las transacciones válidas de las rechazadas.
        validos, rechazados = validar_y_separar(df)

        # Calcular revenue bruto por fila y revenue neto por producto y día.
        validos = calcular_revenue_bruto(validos)
        resumen_diario = calcular_resumen_diario(validos)

        # Guardar los resultados procesados.
        ruta_validos = os.path.join(
            processed_dir,
            "valid.parquet",
        )
        ruta_rechazos = os.path.join(
            processed_dir,
            "rejected.parquet",
        )
        ruta_resumen = os.path.join(
            processed_dir,
            "daily_revenue.parquet",
        )
        ruta_mapa = os.path.join(
            processed_dir,
            "canonical_map.json",
        )

        validos.to_parquet(ruta_validos, index=False)
        rechazados.to_parquet(ruta_rechazos, index=False)
        resumen_diario.to_parquet(ruta_resumen, index=False)

        with open(ruta_mapa, "w", encoding="utf-8") as archivo:
            json.dump(
                mapa_productos,
                archivo,
                ensure_ascii=False,
            )

        return {
            "valid_path": ruta_validos,
            "rejected_path": ruta_rechazos,
            "daily_revenue_path": ruta_resumen,
            "canonical_map_path": ruta_mapa,
            "valid_count": len(validos),
            "rejected_count": len(rechazados),
            "products_count": len(mapa_productos),
            "daily_revenue_count": len(resumen_diario),
            "product_categories": categorias_productos,
        }

    # ── TASK 3: CARGA ─────────────────────────────────────────────────────
    @task
    def load(rutas, **context):
        """Carga productos, transacciones y rechazos en PostgreSQL."""

        import pandas as pd

        id_ejecucion = context["run_id"]

        from airflow.hooks.base import BaseHook

        conexion_dw = BaseHook.get_connection("analytics_db")
        engine = obtener_engine(cadena_conexion=conexion_dw.get_uri())

        try:
            validos = pd.read_parquet(rutas["valid_path"])
            rechazados = pd.read_parquet(rutas["rejected_path"])
            resumen_diario = pd.read_parquet(
                rutas["daily_revenue_path"]
            )

            with open(
                rutas["canonical_map_path"],
                "r",
                encoding="utf-8",
            ) as archivo:
                mapa_productos = json.load(archivo)

            # 1. Cargar la dimensión de productos.
            n_productos = cargar_productos(
                mapa_canonico=mapa_productos,
                engine=engine,
                categorias=rutas["product_categories"],
            )

            # 2. Cargar las transacciones válidas.
            n_transacciones = cargar_transacciones(
                df_validos=validos,
                engine=engine,
                run_id=id_ejecucion,
                archivos_fuente=["data.csv", "online_retail.csv"],
            )

            # 3. Cargar los registros rechazados para auditoría.
            n_rechazos = cargar_rechazos(
                df_rechazados=rechazados,
                engine=engine,
                run_id=id_ejecucion,
                archivos_fuente=["data.csv", "online_retail.csv"],
            )

            n_resumenes = cargar_resumen_diario(
                df_resumen=resumen_diario,
                engine=engine,
            )

            return {
                "productos_cargados": n_productos,
                "transacciones_cargadas": n_transacciones,
                "rechazos_cargados": n_rechazos,
                "resumenes_diarios_cargados": n_resumenes,
            }

        finally:
            engine.dispose()

    # ── ORQUESTACIÓN ─────────────────────────────────────────────────────
    ruta_ventas = extract_sales()
    ruta_historico = extract_history()
    rutas_procesados = transform(ruta_ventas, ruta_historico)
    load(rutas_procesados)


# Registrar el DAG en Airflow.
datamart_etl_ventas()
