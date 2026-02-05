{{config(materialized='incremental', unique_key='business_key')}}

WITH bloco AS (
    SELECT
        business_key,
        sales_type,
        sales_date,
        nf_number,
        cnpj_revenda,
        sku,

        -- agrega dentro do mesmo bloco (load_ts + file_name)
        load_ts,
        SUM(qty) AS qty,
        MAX(file_name) AS file_name
    FROM {{ ref('slv_sellout_asm') }}
    GROUP BY
        business_key,  sales_type, sales_date, nf_number, cnpj_revenda, sku, load_ts, file_name
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
    d.sales_type,
    d.sales_date,
    d.nf_number,
    d.qty,
    d.cnpj_revenda,
    c.cnpj_id,
    d.sku,
    p.product_id,
    d.load_ts,
    d.file_name

FROM dedup d
LEFT JOIN {{ source('bd_samsung_one','d_cnpj') }} c
  ON d.cnpj_revenda COLLATE utf8mb4_unicode_ci
   = c.cnpj       COLLATE utf8mb4_unicode_ci

LEFT JOIN {{ source('bd_samsung_one','dproduct') }} p
  ON d.sku COLLATE utf8mb4_unicode_ci
   = p.sku COLLATE utf8mb4_unicode_ci
WHERE d.rn = 1