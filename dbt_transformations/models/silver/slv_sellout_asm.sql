{{ config(
    materialized='table'
) }}

SELECT 
    file_name,
    load_ts,
    tier2_partnercode,
    endcustomer_code,
    distributor_name,
    sales_type,
    sales_date,
    nf_number,
    sku,
    qty,
    CASE
        WHEN tier2_partnercode IS NOT NULL THEN tier2_partnercode ELSE endcustomer_code 
    END AS  cnpj_revenda,
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
FROM {{ ref('stg_sellout_asm') }}