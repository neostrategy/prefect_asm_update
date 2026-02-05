{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        CAST(file_name AS CHAR(255)) AS file_name,
        CAST(load_ts AS DATETIME) AS load_ts,
        CAST(tier2_partnercode AS CHAR(50)) AS tier2_partnercode,
        CAST(endcustomer_code AS CHAR(50))AS endcustomer_code,

        -- distributor_name em MAIÚSCULO
        UPPER(
            CAST(
                CASE 
                    WHEN tier1_partnername IS NOT NULL THEN tier1_partnername 
                    ELSE distributor
                END AS CHAR(255)
            )
        ) AS distributor_name,

        CAST(sales_type AS CHAR(50)) AS sales_type,

        -- sales_date normalizada como DATE
        CAST(
            CASE
                WHEN sales_date REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN
                    DATE_ADD(
                        '1899-12-30',
                        INTERVAL CAST(FLOOR(sales_date) AS UNSIGNED) DAY
                    )
                WHEN sales_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                    STR_TO_DATE(sales_date, '%Y-%m-%d')
                WHEN sales_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                    STR_TO_DATE(sales_date, '%d/%m/%Y')
                ELSE NULL
            END AS DATE
        ) AS sales_date,

        CAST(nf_number AS CHAR(50)) AS nf_number,
        CAST(sku AS CHAR(50)) AS sku,
        CAST(qty AS DECIMAL(18,2)) AS qty,

        CAST(
            CASE
                WHEN tier2_partnercode IS NOT NULL THEN tier2_partnercode 
                ELSE endcustomer_code 
            END AS CHAR(50)
        ) AS cnpj_revenda,

        CAST(projeto AS CHAR(100)) AS projeto,
        CAST(type AS CHAR(50)) AS type,
        CAST(week AS CHAR(20)) AS week

    FROM {{ ref('stg_sellout_asm') }}
    
)

SELECT
    *,
    -- business_key determinístico
    MD5(
        CONCAT_WS(
            '|',
            CAST(sales_date AS CHAR),
            nf_number,
            distributor_name,
            sku,
            CAST(qty AS CHAR)
        )
    ) AS business_key
FROM base
WHERE distributor_name <> 'NAGEM_BRANDSHOP'
    AND type IN ('LFD','MON','MOBILE','NOTE PC','TABLET','CTV')
