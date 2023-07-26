CREATE
OR REPLACE TABLE abinbev.dim_time AS
SELECT
    DISTINCT `DATE`,
    EXTRACT(
        YEAR
        FROM
            `DATE`
    ) AS YEAR,
    EXTRACT(
        MONTH
        FROM
            `DATE`
    ) AS MONTH,
    EXTRACT(
        DAY
        FROM
            `DATE`
    ) AS DAY,
    PERIOD
FROM
    abinbev.ssot_sales;