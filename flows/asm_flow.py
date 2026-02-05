from tasks.asm_tasks import (s3_read,enrich_raw_metadata,load_raw_mysql,missing_cnpj_check,api_launcher, missing_product_check)
#from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from prefect import flow
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
import subprocess


load_dotenv()

@flow
def asm_flow(s3_block_name: str, s3_key: str, table_name: str):
    """Main flow to read from S3, enrich data, and load into MySQL."""
    df = s3_read(block_name=s3_block_name, key=s3_key)
    #df_enriched = enrich_raw_metadata(df, Path(s3_key).name)
    #load_raw_mysql(df_enriched, table_name)

    #TODO: inserir dbt aqui

    # cnpj_list = missing_cnpj_check()
    # api_launcher(cnpj_list)
    # product_list = missing_product_check()
    # print(product_list)

#TODO: Alterar pois não funciona
#TODO: Preciso reinstalar todas as dependencias
@flow
def run_dbt():
    result = subprocess.run(
        ["dbt","run"],
        capture_output=True,
        text=True
    )
    print("STDOUT:", result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise Exception("dbt run failed")

if __name__ == "__main__":
    asm_flow(
        s3_block_name="aws-credentials-local",
        s3_key="ASM/ASM-2025.csv",
        table_name="sellout_asm_raw"
    )
 