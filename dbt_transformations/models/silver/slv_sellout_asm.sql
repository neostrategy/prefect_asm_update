{{ config(
    materialized='table'
) }}

SELECT 
    *,
    CASE
        WHEN tier2_partnercode IS NOT NULL THEN tier2_partnercode ELSE endcustomer_code 
    END AS  cnpj_revenda
FROM {{ ref('stg_sellout_asm') }}