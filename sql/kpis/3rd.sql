SELECT
    VOLUME_SUM,
    BRAND_NM,
    BTLR_ORG_LVL_C_DESC,
    region_rank
FROM
    (
        SELECT
            SUM(VOLUME) AS VOLUME_SUM,
            b.BRAND_NM,
            s.BTLR_ORG_LVL_C_DESC,
            RANK() OVER (
                PARTITION BY s.BTLR_ORG_LVL_C_DESC
                ORDER BY
                    SUM(s.VOLUME) ASC
            ) AS region_rank
        FROM
            abinbev.ft_sales s
            LEFT JOIN abinbev.dim_brand b ON s.CE_BRAND_FLVR = b.CE_BRAND_FLVR
        GROUP BY
            b.BRAND_NM,
            s.BTLR_ORG_LVL_C_DESC
    )
WHERE
    region_rank = 1
ORDER BY
    BTLR_ORG_LVL_C_DESC