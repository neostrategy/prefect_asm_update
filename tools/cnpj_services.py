from prefect import flow, task
from sqlalchemy import text

def upsert_cnpj_group(engine, data: dict):
    insert_query = text(
    """
    INSERT INTO bd_samsung_one.d_cnpj_group(group_number, legal_name, brand_name)
    VALUES (:group_number, :legal_name, :brand_name)
    ON DUPLICATE KEY UPDATE
        legal_name = VALUES(legal_name),
        brand_name = VALUES(brand_name),
        creation_date = NOW(),
        update_date = NOW(),
        group_id = LAST_INSERT_ID(group_id)
    """
    )
    with engine.begin() as conn:
        cursor = conn.execute(insert_query, data)
    return cursor.lastrowid

def upsert_cnpj(engine, data: dict):
    insert_query = text(
    """
    INSERT INTO bd_samsung_one.d_cnpj(group_id,cnpj, legal_name, brand_name, uf, sefaz_register_status)
    VALUES (:group_id, :cnpj, :legal_name, :brand_name, :uf, :sefaz_register_status)
    ON DUPLICATE KEY UPDATE
        legal_name = VALUES(legal_name),
        brand_name = VALUES(brand_name),
        uf = VALUES(uf),
        sefaz_register_status = VALUES(sefaz_register_status),
        creation_date = NOW(),
        update_date = NOW(),
        cnpj_id = LAST_INSERT_ID(cnpj_id)
    """
    )
    with engine.begin() as conn:
        cursor = conn.execute(insert_query, data)
    return cursor.lastrowid