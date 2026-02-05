from prefect_aws import AwsCredentials, S3Bucket
from prefect_sqlalchemy import SqlAlchemyConnector
from src.helpers import tratamento_cnpj
from tools.api.laucher import DataApiManager
from prefect import task
from datetime import datetime
from sqlalchemy import text
from io import BytesIO
import pandas as pd
from tools.cnpj_services import upsert_cnpj, upsert_cnpj_group

@task
def s3_read(block_name: str, key: str) -> pd.DataFrame:
    """Flow to read data from S3 bucket using Prefect and AWS credentials."""

    aws_credentials = AwsCredentials.load(block_name)
    s3_bucket = S3Bucket(
        bucket_name="canal-azul",
        aws_credentials=aws_credentials
    )

    df_complete = pd.DataFrame()
    s3_objects = s3_bucket.list_objects(folder=key)
    files_paths = [d['Key'] for d in s3_objects if d['Key'].endswith(".csv")]

    for file_path in files_paths:
        print(f"Reading file from S3: {file_path}")
        file_bytes = s3_bucket.read_path(path=file_path)
        df = pd.read_csv(BytesIO(file_bytes), sep=";")
        df['file_name'] = file_path
        # Adding load date YYYY-MM-DD
        df['load_ts'] = datetime.now().strftime("%Y-%m-%d")
        df_complete = pd.concat([df_complete, df.reset_index(drop=True)])
        
    return df_complete

@task
def load_raw_mysql(df: pd.DataFrame, table_name: str):
    """Load the DataFrame into a MySQL database."""
    connector = SqlAlchemyConnector.load("mysql-credentials-local")
    with connector.get_connection(begin=False) as engine:
        df.to_sql(
             name=table_name, 
             con=engine, 
             if_exists='append', 
             index=False
        )
    print(f"Data loaded into table {table_name}.")

@task
def missing_cnpj_check():
    """Placeholder for missing CNPJ check task."""
    connector = SqlAlchemyConnector.load("mysql-credentials-local")
    query = text("""
    WITH cnpjs AS (
        SELECT DISTINCT
            s.cnpj_revenda AS cnpj
        FROM database_samsung.slv_sellout_asm s
        WHERE s.cnpj_revenda IS NOT NULL

        UNION

        SELECT DISTINCT
            s.endcustomer_code AS cnpj
        FROM database_samsung.slv_sellout_asm s
        WHERE s.endcustomer_code IS NOT NULL
    )
    SELECT
        c.cnpj
    FROM cnpjs c
    LEFT JOIN bd_samsung_one.d_cnpj d
        ON c.cnpj COLLATE utf8mb4_unicode_ci
        = d.cnpj COLLATE utf8mb4_unicode_ci
    WHERE d.cnpj IS NULL
    """)
    with connector.get_connection(begin=False) as engine:
        result = engine.execute(query)
        missing_cnpjs = [row[0] for row in result]
    print("Missing CNPJs:", missing_cnpjs)
    return missing_cnpjs

@task
def missing_product_check():
    """Placeholder for missing product check task."""
    connector = SqlAlchemyConnector.load("mysql-credentials-local")

    query = text("""
    SELECT DISTINCT
        s.sku
    FROM database_samsung.slv_sellout_asm s
    LEFT JOIN bd_samsung_one.dproduct d
        ON s.sku COLLATE utf8mb4_unicode_ci
         = d.sku COLLATE utf8mb4_unicode_ci
    WHERE d.sku IS NULL
      AND s.sku IS NOT NULL
    """)

    with connector.get_connection(begin=False) as engine:
        result = engine.execute(query)
        missing_products = [row[0] for row in result]
    print("Missing Products:", missing_products)

    return missing_products

@task
def insert_missing_products(product_list):
    """Placeholder for inserting missing products into the database."""
    connector = SqlAlchemyConnector.load("mysql-credentials-local")

    with connector.get_connection(begin=False) as engine:
        for product in product_list:
            insert_query = text("""
            INSERT INTO bd_samsung_one.dproduct (sku)
            VALUES (:sku)
            """)
            engine.execute(insert_query, {"sku": product})
    print("Inserted missing products into the database.")

@task
#TODO: Alterar esssa função para Lambda AWS e documentar
def api_launcher(cnpj_list):
    
    #TODO: Document this function and modify to Lambda AWS
    #TODO: Desenvolver forma de cadastrar cnpj inválidos
    """Task to launch API calls for a list of CNPJs and process the results."""

    connector = SqlAlchemyConnector.load("mysql-credentials-local")
    engine = connector.get_engine()
    api_manager = DataApiManager()
    results = api_manager.api_launcher(cnpj_list)

    total =len(results["cnpj_data"]["cnpj"])

    for i in range(total):
        
        cnpj_raw = results["cnpj_data"]["cnpj"][i]
        cnpj_treated = tratamento_cnpj(cnpj_raw)
        cnpj_raiz = cnpj_treated[:8]

        cnpj_group_playload = {
            "group_number": cnpj_raiz,
            "legal_name": results["cnpj_data"]["nome"][i],
            "brand_name": results["cnpj_data"]["fantasia"][i]
        }

        # Insert or update CNPJ group
        id = upsert_cnpj_group(engine, cnpj_group_playload)

        cnpj_playload = {
            "group_id": id,
            "cnpj": cnpj_treated, 
            "legal_name": results["cnpj_data"]["nome"][i],
            "brand_name": results["cnpj_data"]["fantasia"][i],
            "uf": results["cnpj_data"]["uf"][i],
            "sefaz_register_status": results["cnpj_data"]["situação"][i]
        }
        print(f"Upserted CNPJ Group ID: {id} for CNPJ Root: {cnpj_raiz}")
        # Insert or update CNPJ data
        upsert_cnpj(engine, cnpj_playload)
         
    return results