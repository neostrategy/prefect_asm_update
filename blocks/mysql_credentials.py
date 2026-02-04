from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
from dotenv import load_dotenv
import os

load_dotenv()

connector = SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=SyncDriver.MYSQL_PYMYSQL,
        username=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        host=os.environ["MYSQL_HOST"],
        port=3306,
        database=os.environ["MYSQL_DATABASE"],
    )
)
connector.save("mysql-credentials-local", overwrite=True)