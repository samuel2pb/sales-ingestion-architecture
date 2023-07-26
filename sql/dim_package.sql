CREATE
OR REPLACE TABLE abinbev.dim_package AS (
    SELECT
        DISTINCT UPPER(
            regexp_replace(TSR_PCKG_NM, '[^a-zA-Z0-9\\s]', '')
        ) as TSR_PCKG_NM,
        PKG_CAT,
        PKG_CAT_DESC
    FROM
        abinbev.ssot_sales
)