{{config(materialized='incremental', unique_key='business_key')}}

WITH bloco AS (
    SELECT
        business_key,
        sales_type,
        sales_date,
        nf_number,
        cnpj_revenda,
        sku,
        distributor_name,
        endcustomer_code,
        week,
        type,
        projeto,
        load_ts,
        SUM(qty) AS qty,
        MAX(file_name) AS file_name
    FROM {{ ref('slv_sellout_asm') }}
    GROUP BY
        business_key,
        sales_type,
        sales_date,
        nf_number,
        cnpj_revenda,
        sku,
        load_ts,
        file_name,
        distributor_name,
        endcustomer_code,
        week,
        type,
        projeto
),

dedup AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY business_key
            ORDER BY load_ts DESC, file_name DESC
        ) AS rn 
    FROM bloco
    
)
SELECT
    d.business_key,
    c.cnpj_id,
    d.cnpj_revenda,
    cf.cnpj_id as endcustomer_cnpj_id,
    d.endcustomer_code,
    p.product_id,
    d.sku,
    d.distributor_name,
    dd.distributor_id,
    
    d.sales_date,
    d.qty,
    d.nf_number,
    d.sales_type,
    
    d.load_ts,
    d.file_name,
    d.week,
    d.projeto

FROM dedup d
LEFT JOIN {{ source('bd_samsung_one','d_cnpj') }} c
  ON d.cnpj_revenda COLLATE utf8mb4_unicode_ci
   = c.cnpj       COLLATE utf8mb4_unicode_ci

LEFT JOIN {{ source('bd_samsung_one','d_cnpj') }} cf
  ON d.endcustomer_code COLLATE utf8mb4_unicode_ci
   = cf.cnpj       COLLATE utf8mb4_unicode_ci

LEFT JOIN {{ source('bd_samsung_one','dproduct') }} p
  ON d.sku COLLATE utf8mb4_unicode_ci
   = p.sku COLLATE utf8mb4_unicode_ci

LEFT JOIN {{ source('bd_samsung_one','ddistributor') }} dd
  ON d.distributor_name COLLATE utf8mb4_unicode_ci
   = dd.name       COLLATE utf8mb4_unicode_ci
WHERE d.rn = 1