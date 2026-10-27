"""Cálculo de revenue bruto y neto por producto y día."""

import logging

import pandas as pd

logger = logging.getLogger("airflow.task")


def calcular_revenue_bruto(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el ingreso bruto de cada transacción."""
    resultado = df.copy()
    resultado["gross_revenue"] = (
        resultado["Quantity"] * resultado["UnitPrice"]
    ).round(4)
    return resultado


def calcular_resumen_diario(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega ventas y devoluciones por fecha y código de producto."""
    columnas = [
        "transaction_date",
        "stock_code",
        "canonical_description",
        "sales_quantity",
        "returns_quantity",
        "net_quantity",
        "gross_revenue",
        "returns_adjustment",
        "net_revenue",
    ]
    if df.empty:
        return pd.DataFrame(columns=columnas)

    claves = ["transaction_date", "StockCode"]
    ventas = (
        df[df["record_type"] == "sale"]
        .groupby(claves, as_index=False)
        .agg(
            sales_quantity=("Quantity", "sum"),
            gross_revenue=("gross_revenue", "sum"),
        )
    )
    devoluciones = (
        df[df["record_type"] == "return_or_adjustment"]
        .groupby(claves, as_index=False)
        .agg(
            returns_quantity=("Quantity", "sum"),
            returns_adjustment=("gross_revenue", "sum"),
        )
    )

    resumen = ventas.merge(
        devoluciones,
        on=claves,
        how="outer",
    )
    cantidades = [
        "sales_quantity",
        "returns_quantity",
        "gross_revenue",
        "returns_adjustment",
    ]
    resumen[cantidades] = resumen[cantidades].fillna(0)
    resumen["net_quantity"] = (
        resumen["sales_quantity"] + resumen["returns_quantity"]
    )
    resumen["net_revenue"] = (
        resumen["gross_revenue"] + resumen["returns_adjustment"]
    )

    descripciones = (
        df.dropna(subset=["StockCode"])
        .drop_duplicates(subset=["StockCode"])
        .set_index("StockCode")["canonical_description"]
    )
    resumen["canonical_description"] = resumen["StockCode"].map(
        descripciones
    )
    resumen = resumen.rename(columns={"StockCode": "stock_code"})
    resumen = resumen[columnas]

    logger.info(
        "Resumen diario calculado: %d filas (producto × día)",
        len(resumen),
    )
    return resumen
