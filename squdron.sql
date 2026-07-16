-- Create FACT_SALES table from sales data at ORDER_ID grain
-- Co-authored with CoCo

-- ============================================================
-- FACT_SALES: one row per order
-- Grain: ORDER_ID
-- Source: DE_SANDBOX.QUICK_REFRESHER.SALES
-- ============================================================

CREATE OR REPLACE TABLE DE_SANDBOX.QUICK_REFRESHER.FACT_SALES (
    ORDER_ID        VARCHAR        NOT NULL PRIMARY KEY,
    SALE_DATE       DATE           NOT NULL,
    REP_NAME        VARCHAR        NOT NULL,
    REGION          VARCHAR        NOT NULL,
    PRODUCT         VARCHAR        NOT NULL,
    AMOUNT          NUMBER(38,2)   NOT NULL,
    UNITS_SOLD      NUMBER(38,0)   NOT NULL,
    UNIT_PRICE      NUMBER(38,2),
    LOADED_AT       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Populate fact table from source
INSERT INTO DE_SANDBOX.QUICK_REFRESHER.FACT_SALES (
    ORDER_ID, SALE_DATE, REP_NAME, REGION, PRODUCT, AMOUNT, UNITS_SOLD, UNIT_PRICE
)
SELECT
    ORDER_ID,
    SALE_DATE,
    REP_NAME,
    REGION,
    PRODUCT,
    AMOUNT,
    UNITS_SOLD,
    DIV0NULL(AMOUNT, UNITS_SOLD) AS UNIT_PRICE
FROM DE_SANDBOX.QUICK_REFRESHER.SALES;
