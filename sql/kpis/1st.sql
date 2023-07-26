SELECT
    VOLUME_SUM,
    TRADE_GROUP_DESC,
    BTLR_ORG_LVL_C_DESC,
    region_rank
FROM
    (
        SELECT
            SUM(s.VOLUME) AS VOLUME_SUM,
            t.TRADE_GROUP_DESC,
            s.BTLR_ORG_LVL_C_DESC,
            RANK() OVER (
                PARTITION BY s.BTLR_ORG_LVL_C_DESC
                ORDER BY
                    SUM(s.VOLUME) DESC
            ) AS region_rank
        FROM
            abinbev.ft_sales s
            LEFT JOIN abinbev.dim_trade t ON s.TRADE_CHNL_DESC = t.TRADE_CHNL_DESC
        GROUP BY
            t.TRADE_GROUP_DESC,
            s.BTLR_ORG_LVL_C_DESC
    )
WHERE
    region_rank <= 3
ORDER BY
    BTLR_ORG_LVL_C_DESC,
    region_rank DESC