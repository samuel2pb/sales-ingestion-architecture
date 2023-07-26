CREATE
OR REPLACE TABLE abinbev.dim_brand AS (
    SELECT
        DISTINCT CE_BRAND_FLVR,
        BRAND_NM
    FROM
        abinbev.ssot_sales
)