"""Extracción de las fuentes del DataMart."""

import logging
from pathlib import Path

import pandas as pd


CSV_ENCODING = "ISO-8859-1"
logger = logging.getLogger("airflow.task")


def extraer_csv(ruta: str | Path, nombre_fuente: str) -> pd.DataFrame:
    """Lee un CSV y conserva la procedencia de cada registro."""
    ruta_archivo = Path(ruta)
    if not ruta_archivo.is_file():
        raise FileNotFoundError(
            f"No se encontró la fuente de datos: {ruta_archivo}"
        )

    logger.info("Iniciando lectura de la fuente: %s", nombre_fuente)
    dataframe = pd.read_csv(ruta_archivo, encoding=CSV_ENCODING)

    # El segundo archivo incluye un índice técnico que no es de negocio.
    dataframe = dataframe.drop(columns=["index"], errors="ignore")

    # Esta columna también se usa en la carga para auditar el registro original.
    dataframe["source_file"] = nombre_fuente
    dataframe["source_row_index"] = range(len(dataframe))

    logger.info(
        "Lectura finalizada para %s. Registros obtenidos: %d",
        nombre_fuente,
        len(dataframe),
    )
    return dataframe


def combinar_fuentes(
    df_principal: pd.DataFrame,
    df_secundario: pd.DataFrame,
) -> pd.DataFrame:
    """Combina las fuentes, manteniendo primero la fuente principal."""
    combinado = pd.concat(
        [df_principal, df_secundario],
        ignore_index=True,
    )
    logger.info(
        "Combinación: %d + %d = %d filas",
        len(df_principal),
        len(df_secundario),
        len(combinado),
    )
    return combinado
