-- =============================================================================
-- INICIALIZACIÓN DEL DATA WAREHOUSE ANALÍTICO
-- Proyecto: MercaAndes Retail Omnicanal – Pipeline Diario de Ventas
-- Se ejecuta automáticamente al crear el contenedor postgres-analytics
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. DIMENSIÓN DE PRODUCTOS
--    Almacena el nombre canónico (moda en mayúsculas) por StockCode.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    stock_code          TEXT PRIMARY KEY,
    canonical_description TEXT NOT NULL,
    category            TEXT        DEFAULT 'Sin categoría',
    is_active           BOOLEAN     DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. TABLA DE HECHOS: TRANSACCIONES VÁLIDAS
--    record_type = 'sale'                 → Quantity > 0
--    record_type = 'return_or_adjustment' → Quantity <= 0
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.fact_transactions (
    transaction_id        BIGSERIAL    PRIMARY KEY,
    run_id                TEXT         NOT NULL,
    source_file           TEXT         NOT NULL,
    source_row_index      BIGINT,
    invoice_no            TEXT         NOT NULL,
    stock_code            TEXT         NOT NULL,
    description           TEXT,
    canonical_description TEXT,
    quantity              NUMERIC(14,3) NOT NULL,
    unit_price            NUMERIC(14,4) NOT NULL,
    gross_revenue         NUMERIC(18,4) NOT NULL,
    record_type           TEXT         NOT NULL
                          CHECK (record_type IN ('sale','return_or_adjustment')),
    transaction_at        TIMESTAMPTZ  NOT NULL,
    transaction_date      DATE         NOT NULL,
    customer_id           TEXT,
    is_guest              BOOLEAN      NOT NULL DEFAULT FALSE,
    country               TEXT,
    created_at            TIMESTAMPTZ  DEFAULT NOW(),
    CONSTRAINT uq_fact_source UNIQUE (source_file, source_row_index)
);

CREATE INDEX IF NOT EXISTS ix_ft_date        ON analytics.fact_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS ix_ft_stock       ON analytics.fact_transactions(stock_code);
CREATE INDEX IF NOT EXISTS ix_ft_record_type ON analytics.fact_transactions(record_type);
CREATE INDEX IF NOT EXISTS ix_ft_customer    ON analytics.fact_transactions(customer_id);
CREATE INDEX IF NOT EXISTS ix_ft_country     ON analytics.fact_transactions(country);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. AUDITORÍA: REGISTROS RECHAZADOS
--    Ventas con precio unitario <= 0 o nulo, etc.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.rejected_records (
    rejection_id      BIGSERIAL    PRIMARY KEY,
    run_id            TEXT         NOT NULL,
    source_file       TEXT         NOT NULL,
    source_row_index  BIGINT,
    invoice_no        TEXT,
    stock_code        TEXT,
    quantity          NUMERIC(14,3),
    unit_price        NUMERIC(14,4),
    customer_id       TEXT,
    rejection_reason  TEXT         NOT NULL,
    raw_record        JSONB        NOT NULL,
    rejected_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_rr_run    ON audit.rejected_records(run_id);
CREATE INDEX IF NOT EXISTS ix_rr_reason ON audit.rejected_records(rejection_reason);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. TABLA AGREGADA: REVENUE DIARIO POR PRODUCTO (BRUTO Y NETO)
--    net_revenue = gross_revenue + returns_adjustment  (ajuste es negativo)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.daily_product_revenue (
    summary_id            BIGSERIAL    PRIMARY KEY,
    transaction_date      DATE         NOT NULL,
    stock_code            TEXT         NOT NULL,
    canonical_description TEXT,
    sales_quantity        NUMERIC(14,3) DEFAULT 0,
    returns_quantity      NUMERIC(14,3) DEFAULT 0,
    net_quantity          NUMERIC(14,3) DEFAULT 0,
    gross_revenue         NUMERIC(18,4) DEFAULT 0,
    returns_adjustment    NUMERIC(18,4) DEFAULT 0,
    net_revenue           NUMERIC(18,4) DEFAULT 0,
    updated_at            TIMESTAMPTZ   DEFAULT NOW(),
    CONSTRAINT uq_daily_rev UNIQUE (transaction_date, stock_code)
);

CREATE INDEX IF NOT EXISTS ix_dr_date  ON analytics.daily_product_revenue(transaction_date);
CREATE INDEX IF NOT EXISTS ix_dr_stock ON analytics.daily_product_revenue(stock_code);

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. VISTAS ANALÍTICAS (Power BI / Reportes)
-- ─────────────────────────────────────────────────────────────────────────────

-- 5.1 Ventas netas mensuales
CREATE OR REPLACE VIEW analytics.vw_monthly_net_sales AS
SELECT
    DATE_TRUNC('month', transaction_date)::DATE AS sales_month,
    SUM(gross_revenue)      AS total_gross_revenue,
    SUM(returns_adjustment) AS total_returns,
    SUM(net_revenue)        AS total_net_revenue,
    SUM(sales_quantity)     AS total_units_sold,
    SUM(returns_quantity)   AS total_units_returned,
    SUM(net_quantity)       AS total_net_units,
    ROUND(ABS(SUM(returns_adjustment))
          / NULLIF(SUM(gross_revenue),0) * 100, 2) AS return_rate_pct
FROM analytics.daily_product_revenue
GROUP BY 1 ORDER BY 1;

-- 5.2 Impacto de transacciones anónimas vs identificadas
CREATE OR REPLACE VIEW analytics.vw_customer_impact AS
SELECT
    CASE WHEN is_guest THEN 'Anónimo' ELSE 'Identificado' END AS segmento,
    record_type,
    COUNT(*)           AS total_txn,
    SUM(quantity)      AS total_units,
    SUM(gross_revenue) AS total_revenue
FROM analytics.fact_transactions
GROUP BY 1, 2 ORDER BY 1, 2;

-- 5.3 Resumen de rechazos por motivo
CREATE OR REPLACE VIEW audit.vw_rejection_summary AS
SELECT
    run_id,
    source_file,
    rejection_reason,
    COUNT(*)          AS qty,
    MIN(rejected_at)  AS first_at,
    MAX(rejected_at)  AS last_at
FROM audit.rejected_records
GROUP BY 1, 2, 3 ORDER BY qty DESC;
