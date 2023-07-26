SELECT
    SUM(VOLUME) AS VOLUME_SUM,
    b.BRAND_NM,
    t.YEAR,
    t.MONTH
FROM
    abinbev.ft_sales s
    LEFT JOIN abinbev.dim_brand b ON s.CE_BRAND_FLVR = b.CE_BRAND_FLVR
    LEFT JOIN abinbev.dim_time t ON s.DATE = t.DATE
GROUP BY
    b.BRAND_NM,
    t.MONTH,
    t.YEAR
ORDER BY
    BRAND_NM,
    YEAR ASC,
    MONTH ASC