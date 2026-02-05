{{ config(
    materialized = 'view'
) }}

SELECT
    regexp_replace(tier2_partnercode, '[^0-9]','') as tier2_partnercode,
    regexp_replace(endcustomer_code, '[^0-9]','') as endcustomer_code,
    tier1_partnername,
    distributor,
    salestype as sales_type,
    salesdate as sales_date,
    invoice_number as nf_number,
    PN as sku,
    qty,
    projeto,
    type,
    week,
    file_name,
    load_ts
FROM {{ source('raw', 'sellout_asm_raw') }}
    
