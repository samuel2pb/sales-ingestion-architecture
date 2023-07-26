/* The best scenario is to have an id column what is not provided in this case */
MERGE INTO abinbev.ft_sales AS target USING (
    SELECT
        ID,
        `DATE`,
        CE_BRAND_FLVR,
        UPPER(
            regexp_replace(TSR_PCKG_NM, '[^a-zA-Z0-9\\s]', '')
        ) as TSR_PCKG_NM,
        TRADE_CHNL_DESC,
        BTLR_ORG_LVL_C_DESC,
        VOLUME
    FROM
        abinbev.ssot_sales
) AS source ON target.ID = source.ID
WHEN MATCHED THEN
UPDATE
SET
    target.DATE = source.DATE,
    target.CE_BRAND_FLVR = source.CE_BRAND_FLVR,
    target.TSR_PCKG_NM = source.TSR_PCKG_NM,
    target.TRADE_CHNL_DESC = source.TRADE_CHNL_DESC,
    target.BTLR_ORG_LVL_C_DESC = source.BTLR_ORG_LVL_C_DESC,
    target.VOLUME = source.VOLUME
    WHEN NOT MATCHED THEN
INSERT
    (
        DATE,
        CE_BRAND_FLVR,
        TSR_PCKG_NM,
        TRADE_CHNL_DESC,
        BTLR_ORG_LVL_C_DESC,
        VOLUME
    )
VALUES
    (
        source.DATE,
        source.CE_BRAND_FLVR,
        source.TSR_PCKG_NM,
        source.TRADE_CHNL_DESC,
        source.BTLR_ORG_LVL_C_DESC,
        source.VOLUME
    );