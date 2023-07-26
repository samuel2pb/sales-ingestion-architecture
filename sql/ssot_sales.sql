/* The best scenario is to have a id column what is not provided in this case */
MERGE INTO abinbev.ssot_sales AS target USING abinbev.stage_sales AS source ON target.ID = source.ID
WHEN MATCHED THEN
UPDATE
SET
    target.TRADE_CHNL_DESC = source.TRADE_CHNL_DESC,
    target.DATE = source.DATE,
    target.CE_BRAND_FLVR = source.CE_BRAND_FLVR,
    target.BRAND_NM = source.BRAND_NM,
    target.BTLR_ORG_LVL_C_DESC = source.BTLR_ORG_LVL_C_DESC,
    target.CHNL_GROUP = source.CHNL_GROUP,
    target.PKG_CAT = source.PKG_CAT,
    target.PKG_CAT_DESC = source.PKG_CAT_DESC,
    target.TSR_PCKG_NM = source.TSR_PCKG_NM,
    target.VOLUME = source.VOLUME,
    target.YEAR = source.YEAR,
    target.MONTH = source.MONTH,
    target.PERIOD = source.PERIOD,
    target.TRADE_GROUP_DESC = source.TRADE_GROUP_DESC,
    target.TRADE_TYPE_DESC = source.TRADE_TYPE_DESC
    WHEN NOT MATCHED THEN
INSERT
    (
        TRADE_CHNL_DESC,
        DATE,
        CE_BRAND_FLVR,
        BRAND_NM,
        BTLR_ORG_LVL_C_DESC,
        CHNL_GROUP,
        PKG_CAT,
        PKG_CAT_DESC,
        TSR_PCKG_NM,
        VOLUME,
        YEAR,
        MONTH,
        PERIOD,
        TRADE_GROUP_DESC,
        TRADE_TYPE_DESC
    )
VALUES
    (
        source.TRADE_CHNL_DESC,
        source.DATE,
        source.CE_BRAND_FLVR,
        source.BRAND_NM,
        source.BTLR_ORG_LVL_C_DESC,
        source.CHNL_GROUP,
        source.PKG_CAT,
        source.PKG_CAT_DESC,
        source.TSR_PCKG_NM,
        source.VOLUME,
        source.YEAR,
        source.MONTH,
        source.PERIOD,
        source.TRADE_GROUP_DESC,
        source.TRADE_TYPE_DESC
    );

DROP TABLE IF EXISTS abinbev.stage_sales;