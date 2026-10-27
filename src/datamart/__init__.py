
"""Paquete de lógica de negocio del ETL (sin dependencia de Airflow).

Módulos separados por responsabilidad:
  - extract: lectura de CSV y combinación de fuentes
  - transform: deduplicación, códigos, fechas, clasificación, customer ID y descripciones
  - validate: validación de precios y separación de rechazos
  - revenue: cálculo del revenue bruto
  - load: conexión y carga a PostgreSQL
"""

from .extract import (
    extraer_csv,
    combinar_fuentes,
)

from .transform import (
    normalize_source,
    choose_canonical_descriptions,
    categorizar_productos,
    transform_sources,
    deduplicar,
    normalizar_codigos,
    estandarizar_fechas_utc,
    clasificar_registros,
    limpiar_customer_id,
    construir_mapa_canonico,
    aplicar_descripcion_canonica,
)

from .validate import validar_y_separar

from .revenue import calcular_revenue_bruto, calcular_resumen_diario

from .load import (
    obtener_engine,
    cargar_transacciones,
    cargar_rechazos,
    cargar_productos,
    cargar_resumen_diario,
)

__all__ = [
    "extraer_csv",
    "combinar_fuentes",
    "normalize_source",
    "choose_canonical_descriptions",
    "categorizar_productos",
    "transform_sources",
    "deduplicar",
    "normalizar_codigos",
    "estandarizar_fechas_utc",
    "clasificar_registros",
    "limpiar_customer_id",
    "construir_mapa_canonico",
    "aplicar_descripcion_canonica",
    "validar_y_separar",
    "calcular_revenue_bruto",
    "calcular_resumen_diario",
    "obtener_engine",
    "cargar_transacciones",
    "cargar_rechazos",
    "cargar_productos",
    "cargar_resumen_diario",
]
