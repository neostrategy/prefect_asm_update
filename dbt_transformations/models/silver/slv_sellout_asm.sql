{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        file_name,
        load_ts,
        tier2_partnercode,
        endcustomer_code,
        CASE 
            WHEN tier1_partnername IS NOT NULL THEN tier1_partnername 
            ELSE distributor
        END AS distributor_name,
        sales_type,
        sales_date,
        nf_number,
        sku,
        qty,
        CASE
            WHEN tier2_partnercode IS NOT NULL THEN tier2_partnercode 
            ELSE endcustomer_code 
        END AS cnpj_revenda,
        projeto,
        type,
        week
    FROM {{ ref('stg_sellout_asm') }}
    WHERE type IN ("LFD","MON","MOBILE","NOTE PC","TABLET")
)

SELECT
    *,
    MD5(
        CONCAT_WS(
            '|',
            sales_date,
            nf_number,
            distributor_name,
            sku,
            qty
        )
    ) AS business_key
FROM base
