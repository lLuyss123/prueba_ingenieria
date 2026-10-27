"""
Validación y separación de registros.
DataMart S.A.S. – ETL de ventas.

Sin dependencias de Airflow: funciones puras con pandas.
"""

import logging

import numpy as np
import pandas as pd

RECORD_TYPE_SALE = "sale"
logger = logging.getLogger("airflow.task")


def validar_y_separar(df):
    """Separa los registros en dos grupos: válidos y rechazados.

    Regla de negocio:
      → Una VENTA con precio unitario cero, negativo o vacío se RECHAZA.
      → Las devoluciones/ajustes NO se rechazan por precio (se necesitan
        para calcular el revenue neto).

    Retorna
    -------
    (validos, rechazados) : tupla de dos DataFrames
    """
    es_venta = df["record_type"] == RECORD_TYPE_SALE
    invoice_missing = (
        df["InvoiceNo"].isna()
        | df["InvoiceNo"].astype("string").str.strip().eq("")
    )
    stock_missing = (
        df["StockCode"].isna()
        | df["StockCode"].astype("string").str.strip().eq("")
    )
    date_missing = df["transaction_at"].isna()
    quantity_missing = df["Quantity"].isna()
    price_missing = df["UnitPrice"].isna()
    sale_price_invalid = es_venta & df["UnitPrice"].le(0)

    condiciones = [
        invoice_missing,
        stock_missing,
        date_missing,
        quantity_missing,
        price_missing,
        sale_price_invalid,
    ]
    condiciones = [
        condicion.to_numpy(dtype=bool, na_value=False)
        for condicion in condiciones
    ]
    motivos = [
        "Número de factura nulo o vacío",
        "Código de producto nulo o vacío",
        "Fecha de transacción inválida",
        "Cantidad nula o inválida",
        "Precio unitario nulo o inválido",
        "Venta con precio unitario cero o negativo",
    ]
    razon_rechazo = pd.Series(
        np.select(condiciones, motivos, default=""),
        index=df.index,
        dtype="string",
    )
    debe_rechazarse = razon_rechazo.ne("")

    rechazados = df[debe_rechazarse].copy()
    rechazados["rejection_reason"] = razon_rechazo[debe_rechazarse]
    validos = df[~debe_rechazarse].copy()

    logger.info(
        "Validación: %d válidos  /  %d rechazados",
        len(validos), len(rechazados),
    )
    return validos, rechazados
