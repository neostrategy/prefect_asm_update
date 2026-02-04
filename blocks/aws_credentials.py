from prefect_aws import AwsCredentials
from dotenv import load_dotenv
import os

load_dotenv()

AwsCredentials(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-west-2",
).save("aws-credentials-local", overwrite=True)
