import pandas as pd
from scraper.config import s3_client, S3_BUCKET
import pyarrow.parquet as pq
import pyarrow as pa
import io

def test_upload():
    data = [
        {"segment": None, "cod": "ALOS3", "asset": "ALLOS", "type": "ON NM", "part": "0,491", "partAcum": None, "theoricalQty": "476.976.044"},
        {"segment": None, "cod": "ABEV3", "asset": "AMBEV S/A", "type": "ON", "part": "2,780", "partAcum": None, "theoricalQty": "4.394.835.131"},
        {"segment": None, "cod": "ASAI3", "asset": "ASSAI", "type": "ON NM", "part": "0,639", "partAcum": None, "theoricalQty": "1.345.897.506"},
        {"segment": None, "cod": "AURE3", "asset": "AUREN", "type": "ON NM", "part": "0,140", "partAcum": None, "theoricalQty": "323.738.747"},
    ]

    df = pd.DataFrame(data)

    df["part"] = df["part"].str.replace(",", ".").astype(float)
    df["theoricalQty"] = df["theoricalQty"].str.replace(".", "").astype(int)

    if df["partAcum"].notnull().any():
        df["partAcum"] = df["partAcum"].str.replace(",", ".").astype(float)

    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3_key = "test/realistic_data.parquet"

    s3_client.upload_fileobj(buffer, S3_BUCKET, s3_key)
    print(f"Upload successful to s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    test_upload()
