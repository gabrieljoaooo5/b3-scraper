import boto3
import pandas as pd
import io
import os
from scraper.config import S3_BUCKET

def validate_parquet(s3_key):
    session = boto3.Session(profile_name=os.getenv("AWS_PROFILE", "default"))
    s3_client = session.client("s3")

    buffer = io.BytesIO()
    s3_client.download_fileobj(S3_BUCKET, s3_key, buffer)
    buffer.seek(0)

    df = pd.read_parquet(buffer)

    print(f"Data preview from s3://{S3_BUCKET}/{s3_key}:\n")
    print(df.head())

    print("\nDataFrame info:")
    print(df.info())

    print("\nDataFrame description:")
    print(df.describe(include='all'))

if __name__ == "__main__":
    s3_key = "raw/date=2025-07-12/b3_data.parquet"
    validate_parquet(s3_key)
