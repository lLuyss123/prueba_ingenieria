"""Transformación y limpieza de datos de ventas."""

import logging

import numpy as np
import pandas as pd


DEDUP_KEY = [
    "InvoiceNo",
    "StockCode",
    "InvoiceDate",
    "Quantity",
    "UnitPrice",
    "CustomerID",
    "Country",
]
CUSTOMER_GUEST = "GUEST"
RECORD_TYPE_SALE = "sale"
RECORD_TYPE_RETURN = "return_or_adjustment"
CATEGORY_UNCLASSIFIED = "Sin categoría"
CATEGORY_PATTERNS = {
    "Papelería": (
        r"\b(?:PEN|PENCIL|NOTEBOOK|STATIONERY|RULER|ERASER|ENVELOPE|"
        r"POSTCARD|DIARY|CALENDAR|SCRAPBOOK|PAPER|CRAYON)\b"
    ),
    "Ropa": (
        r"\b(?:T[- ]?SHIRTS?|SHIRTS?|DRESSES?|SKIRTS?|TROUSERS?|JEANS?|"
        r"SOCKS?|SCARVES?|HATS?|GLOVES?|JUMPERS?|CARDIGANS?|COATS?|"
        r"JACKETS?|APRONS?|UNDERWEAR)\b"
    ),
    "Deportes": (
        r"\b(?:SPORT|YOGA|FITNESS|TENNIS|FOOTBALL|GOLF|CYCL(?:E|ING)|"
        r"BICYCLE|SWIM|BALL|EXERCISE|DUMBBELL|CAMPING|BADMINTON|CRICKET)\b"
    ),
    "Electrónica": (
        r"\b(?:USB|BATTERY|CHARGER|HEADPHONES?|EARPHONES?|KEYBOARD|"
        r"MOUSE|CAMERA|COMPUTER|LAPTOP|PHONE|MOBILE|SPEAKER|RADIO|"
        r"TELEPHONE)\b"
    ),
    "Hogar": (
        r"\b(?:MUGS?|CUPS?|PLATES?|BOWLS?|KITCHEN|CANDLES?|LAMPS?|"
        r"VASES?|CUSHIONS?|BASKETS?|MIRRORS?|DOORS?|GARDEN|CLOCKS?|"
        r"TOWELS?|SHELVES|HOLDERS?|HANGING|TEA|JUGS?|GLASSES?|HEARTS?|"
        r"DECORATION|CUTLERY|TRAYS?|POTS?|CERAMIC|STORAGE|LIGHTS?|"
        r"BOXES|BAGS?|TINS?|BOTTLES?|LUNCH|ORNAMENTS?|WREATHS?|RACKS?|"
        r"BATH|HOOKS?|PLANTERS?)\b"
    ),
}
logger = logging.getLogger("airflow.task")


def normalize_source(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normaliza una fuente sin modificar el DataFrame original."""
    dataframe = dataframe.copy()
    dataframe = dataframe.drop(columns=["index"], errors="ignore")

    dataframe["StockCode"] = (
        dataframe["StockCode"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    dataframe["Description"] = (
        dataframe["Description"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    dataframe = limpiar_customer_id(dataframe)
    dataframe["InvoiceNo"] = dataframe["InvoiceNo"].astype("string")
    dataframe["InvoiceDate"] = pd.to_datetime(
        dataframe["InvoiceDate"],
        format="%m/%d/%Y %H:%M",
        errors="coerce",
    )
    dataframe["Quantity"] = pd.to_numeric(
        dataframe["Quantity"],
        errors="coerce",
    )
    dataframe["UnitPrice"] = pd.to_numeric(
        dataframe["UnitPrice"],
        errors="coerce",
    )

    dataframe["transaction_at"] = pd.to_datetime(
        dataframe["InvoiceDate"],
        utc=True,
        errors="coerce",
    )
    dataframe["transaction_date"] = dataframe["transaction_at"].dt.date
    dataframe = clasificar_registros(dataframe)

    fechas_invalidas = dataframe["transaction_at"].isna().sum()
    if fechas_invalidas:
        logger.warning("%d fechas no se pudieron parsear", fechas_invalidas)

    return dataframe


def choose_canonical_descriptions(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Elige la descripción más frecuente para cada código de producto."""
    return aplicar_descripcion_canonica(
        dataframe,
        construir_mapa_canonico(dataframe),
    )


def categorizar_productos(descripciones: pd.Series) -> pd.Series:
    """Asigna una categoría aproximada mediante palabras clave conocidas."""
    texto = descripciones.astype("string").fillna("").str.upper()
    condiciones = [
        texto.str.contains(patron, regex=True, na=False).to_numpy(
            dtype=bool,
            na_value=False,
        )
        for patron in CATEGORY_PATTERNS.values()
    ]
    return pd.Series(
        np.select(
            condiciones,
            list(CATEGORY_PATTERNS),
            default=CATEGORY_UNCLASSIFIED,
        ),
        index=descripciones.index,
        dtype="string",
    )


def transform_sources(
    sales: pd.DataFrame,
    history: pd.DataFrame,
) -> pd.DataFrame:
    """Normaliza, unifica y deduplica las fuentes priorizando data.csv."""
    sales = normalize_source(sales)
    history = normalize_source(history)

    combined = pd.concat(
        [sales, history],
        ignore_index=True,
    )
    combined["source_priority"] = (
        combined["source_file"]
        .map({"data.csv": 0, "online_retail.csv": 1})
        .fillna(2)
    )
    combined = combined.sort_values(
        "source_priority",
        kind="stable",
    )
    combined = combined.drop_duplicates(
        subset=DEDUP_KEY,
        keep="first",
    )

    combined = choose_canonical_descriptions(combined)
    combined = combined.drop(columns=["source_priority"])

    logger.info(
        "Transformación completada: %d registros unificados",
        len(combined),
    )
    return combined


def deduplicar(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina duplicados según la clave compuesta de transacción."""
    filas_antes = len(df)
    resultado = df.drop_duplicates(subset=DEDUP_KEY, keep="first")
    logger.info(
        "Deduplicación: %d → %d (removidos %d)",
        filas_antes,
        len(resultado),
        filas_antes - len(resultado),
    )
    return resultado.reset_index(drop=True)


def normalizar_codigos(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza los códigos de producto."""
    resultado = df.copy()
    resultado["StockCode"] = (
        resultado["StockCode"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    return resultado


def estandarizar_fechas_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Crea las columnas de fecha UTC y fecha de transacción."""
    resultado = df.copy()
    resultado["transaction_at"] = pd.to_datetime(
        resultado["InvoiceDate"],
        format="%m/%d/%Y %H:%M",
        utc=True,
        errors="coerce",
    )
    resultado["transaction_date"] = resultado["transaction_at"].dt.date

    fechas_invalidas = resultado["transaction_at"].isna().sum()
    if fechas_invalidas:
        logger.warning("%d fechas no se pudieron parsear", fechas_invalidas)
    return resultado


def clasificar_registros(df: pd.DataFrame) -> pd.DataFrame:
    """Clasifica cada transacción como venta o devolución/ajuste."""
    resultado = df.copy()
    resultado["record_type"] = np.where(
        resultado["Quantity"] > 0,
        RECORD_TYPE_SALE,
        RECORD_TYPE_RETURN,
    )
    ventas = (resultado["record_type"] == RECORD_TYPE_SALE).sum()
    logger.info(
        "Clasificación: %d ventas, %d devoluciones/ajustes",
        ventas,
        len(resultado) - ventas,
    )
    return resultado


def limpiar_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza los identificadores y etiqueta los clientes sin ID."""
    resultado = df.copy()
    customer_id = (
        resultado["CustomerID"]
        .astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    customer_id = customer_id.fillna("")
    resultado["CustomerID"] = pd.Series(
        np.where(customer_id.eq(""), CUSTOMER_GUEST, customer_id),
        index=resultado.index,
        dtype="string",
    )
    logger.info(
        "CustomerID: %d registros de invitados",
        resultado["CustomerID"].eq(CUSTOMER_GUEST).sum(),
    )
    return resultado


def construir_mapa_canonico(df: pd.DataFrame) -> dict[str, str]:
    """Construye el mapa de códigos a descripciones canónicas."""
    descripciones = df[["StockCode", "Description"]].copy()
    descripciones["StockCode"] = (
        descripciones["StockCode"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    descripciones["Description"] = (
        descripciones["Description"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    return (
        descripciones.dropna(subset=["StockCode", "Description"])
        .groupby(["StockCode", "Description"])
        .size()
        .rename("frequency")
        .reset_index()
        .sort_values(
            ["StockCode", "frequency", "Description"],
            ascending=[True, False, True],
        )
        .drop_duplicates(subset=["StockCode"])
        .set_index("StockCode")["Description"]
        .to_dict()
    )


def aplicar_descripcion_canonica(
    df: pd.DataFrame,
    mapa: dict[str, str],
) -> pd.DataFrame:
    """Agrega la descripción canónica de cada producto."""
    resultado = df.copy()
    stock_code = (
        resultado["StockCode"]
        .astype("string")
        .str.strip()
        .str.upper()
    )
    resultado["canonical_description"] = (
        stock_code.map(mapa).fillna("UNKNOWN")
    )
    resultado["category"] = categorizar_productos(
        resultado["canonical_description"]
    )
    return resultado
