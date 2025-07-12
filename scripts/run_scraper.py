from scraper.scraper import fetch_b3_data
from scraper.config import s3_client, S3_BUCKET
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime

def main():
    df = fetch_b3_data()

    today_str = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"raw/date={today_str}/b3_data.parquet"

    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3_client.upload_fileobj(buffer, S3_BUCKET, s3_key)
    print(f"Upload complete: s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    main()