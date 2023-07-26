CREATE
OR REPLACE TABLE abinbev.dim_trade AS (
    SELECT
        DISTINCT TRADE_CHNL_DESC,
        CHNL_GROUP,
        TRADE_GROUP_DESC,
        TRADE_TYPE_DESC
    FROM
        abinbev.ssot_sales
)