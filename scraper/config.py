import os
from dotenv import load_dotenv
import boto3

load_dotenv()

AWS_PROFILE = os.getenv("AWS_PROFILE", "default")
S3_BUCKET = os.getenv("S3_BUCKET")

if not S3_BUCKET:
    raise ValueError("S3_BUCKET must be set in the .env file")

session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client("s3")
