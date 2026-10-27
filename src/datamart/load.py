"""
Carga de datos limpios al Data Warehouse PostgreSQL.
DataMart S.A.S. – ETL de ventas.

Sin dependencias de Airflow: funciones puras con SQLAlchemy y pandas.
"""

import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text, types as sa_types

CUSTOMER_GUEST = "GUEST"
logger = logging.getLogger("airflow.task")


def obtener_engine(cadena_conexion):
    """Crea una conexión a la base de datos PostgreSQL.

    Recibe la cadena de conexión (ej: 'postgresql+psycopg2://user:pass@host/db')
    y devuelve un engine de SQLAlchemy listo para usar.
    """
    if cadena_conexion.startswith("postgres://"):
        cadena_conexion = cadena_conexion.replace(
            "postgres://",
            "postgresql://",
            1,
        )
    return create_engine(cadena_conexion)


def cargar_transacciones(
    df_validos,
    engine,
    run_id,
    archivos_fuente=None,
):
    """Carga las transacciones válidas a la tabla analytics.fact_transactions.

    Para evitar duplicados si se re-ejecuta el DAG, primero BORRA las filas
    del mismo archivo fuente y luego inserta las nuevas.
    """
    archivos_fuente = archivos_fuente or (
        df_validos["source_file"].dropna().unique().tolist()
    )
    with engine.begin() as conexion:
        for archivo in archivos_fuente:
            conexion.execute(
                text(
                    "DELETE FROM analytics.fact_transactions "
                    "WHERE source_file = :sf"
                ),
                {"sf": archivo},
            )

        if df_validos.empty:
            logger.warning("Sin transacciones válidas para cargar")
            return 0

        registros = pd.DataFrame({
            "run_id":                run_id,
            "source_file":           df_validos["source_file"],
            "source_row_index":      df_validos["source_row_index"],
            "invoice_no":            df_validos["InvoiceNo"].astype(str),
            "stock_code":            df_validos["StockCode"],
            "description":           df_validos["Description"],
            "canonical_description": df_validos["canonical_description"],
            "quantity":              df_validos["Quantity"],
            "unit_price":            df_validos["UnitPrice"],
            "gross_revenue":         df_validos["gross_revenue"],
            "record_type":           df_validos["record_type"],
            "transaction_at":        df_validos["transaction_at"],
            "transaction_date":      df_validos["transaction_date"],
            "customer_id":           df_validos["CustomerID"],
            "is_guest":              df_validos["CustomerID"].eq(CUSTOMER_GUEST),
            "country":               df_validos["Country"],
        })
        registros.to_sql(
            name="fact_transactions",
            con=conexion,
            schema="analytics",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

    logger.info("Cargadas %d transacciones → fact_transactions", len(registros))
    return len(registros)


def cargar_rechazos(
    df_rechazados,
    engine,
    run_id,
    archivos_fuente=None,
):
    """Carga los registros rechazados a la tabla audit.rejected_records.

    Cada rechazo incluye el motivo y una copia del registro original en JSON
    para poder revisarlo después.

    Reemplaza los rechazos por archivo fuente para hacer idempotentes los reintentos.
    """
    def fila_a_json(fila):
        return json.dumps({
            "InvoiceNo":   str(fila.get("InvoiceNo", "")),
            "StockCode":   str(fila.get("StockCode", "")),
            "Description": str(fila.get("Description", "")),
            "Quantity":    float(fila["Quantity"]) if pd.notna(fila["Quantity"]) else None,
            "UnitPrice":   float(fila["UnitPrice"]) if pd.notna(fila["UnitPrice"]) else None,
            "InvoiceDate": str(fila.get("InvoiceDate", "")),
            "CustomerID":  str(fila["CustomerID"]) if pd.notna(fila.get("CustomerID")) else None,
            "Country":     str(fila.get("Country", "")),
        }, ensure_ascii=False)

    archivos_fuente = archivos_fuente or (
        df_rechazados["source_file"].dropna().unique().tolist()
    )
    with engine.begin() as conexion:
        for archivo in archivos_fuente:
            conexion.execute(
                text(
                    "DELETE FROM audit.rejected_records "
                    "WHERE source_file = :sf"
                ),
                {"sf": archivo},
            )

        if df_rechazados.empty:
            logger.info("Sin registros rechazados")
            return 0

        registros = pd.DataFrame({
            "run_id":           run_id,
            "source_file":      df_rechazados["source_file"],
            "source_row_index": df_rechazados["source_row_index"],
            "invoice_no":       df_rechazados["InvoiceNo"].astype("string"),
            "stock_code":       df_rechazados["StockCode"],
            "quantity":         df_rechazados["Quantity"],
            "unit_price":       df_rechazados["UnitPrice"],
            "customer_id":      df_rechazados["CustomerID"],
            "rejection_reason": df_rechazados["rejection_reason"],
            "raw_record":       df_rechazados.apply(fila_a_json, axis=1),
        })

        registros.to_sql(
            name="rejected_records",
            con=conexion,
            schema="audit",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
            dtype={"raw_record": sa_types.JSON()},
        )

    logger.info("Cargados %d rechazos → rejected_records", len(registros))
    return len(registros)


def cargar_productos(mapa_canonico, engine, categorias=None):
    """Carga la tabla de productos (analytics.dim_products).

    Hace un full-refresh: borra todo y vuelve a cargar desde el mapa canónico.
    """
    productos = pd.DataFrame([
        {
            "stock_code": codigo,
            "canonical_description": nombre,
            "category": (categorias or {}).get(codigo, "Sin categoría"),
        }
        for codigo, nombre in mapa_canonico.items()
    ])

    with engine.begin() as conexion:
        conexion.execute(text("TRUNCATE TABLE analytics.dim_products"))

        if not productos.empty:
            productos.to_sql(
                name="dim_products",
                con=conexion,
                schema="analytics",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )

    logger.info("Cargados %d productos → dim_products", len(productos))
    return len(productos)


def cargar_resumen_diario(df_resumen, engine):
    """Carga el resumen de revenue diario (analytics.daily_product_revenue).

    Hace un full-refresh: borra todo y vuelve a cargar.
    """
    with engine.begin() as conexion:
        conexion.execute(text("TRUNCATE TABLE analytics.daily_product_revenue"))

        if not df_resumen.empty:
            df_resumen.to_sql(
                name="daily_product_revenue",
                con=conexion,
                schema="analytics",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )

    logger.info("Cargadas %d filas → daily_product_revenue", len(df_resumen))
    return len(df_resumen)
