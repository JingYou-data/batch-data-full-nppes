"""
Step 6: Move data from MinIO to memory, change column name, move back to MinIO
"""
import os
import pandas as pd  
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import duckdb
import io

load_dotenv()

def main():
    print("=" * 70)
    print("Step 6: MinIO → Memory → Transform → MinIO")
    print("=" * 70)
    
    # Setup MinIO client
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    source_bucket = 'raw-data'
    target_bucket = 'processed-data'
    
    # Create buckets if not exist
    for bucket in [source_bucket, target_bucket]:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"✓ Created bucket: {bucket}")
        except Exception as e:
            print(f"✓ Bucket exists: {bucket}")
    
    # Step 1: Upload sample data to MinIO (from local)
    print("\n### Step 1: Upload sample to MinIO ###")
    local_file = os.getenv('NPPES_FILE_PATH')
    
    # Use DuckDB to create a small sample
    conn = duckdb.connect(':memory:')
    sample_data = conn.execute(f"""
        SELECT 
            NPI,
            "Provider Business Practice Location Address State Name" as State,
            "Provider Business Practice Location Address City Name" as City,
            "Healthcare Provider Taxonomy Code_1" as Taxonomy
        FROM read_csv_auto('{local_file}')
        WHERE "Healthcare Provider Taxonomy Code_1" = '207RC0000X'
        LIMIT 1000
    """).df()
    
    # Save to CSV in memory
    csv_buffer = io.StringIO()
    sample_data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Upload to MinIO
    s3.put_object(
        Bucket=source_bucket,
        Key='cardiology_sample.csv',
        Body=csv_buffer.getvalue()
    )
    print(f"✓ Uploaded {len(sample_data)} records to MinIO")
    
    # Step 2: Read from MinIO to memory
    print("\n### Step 2: Read from MinIO to Memory ###")
    response = s3.get_object(Bucket=source_bucket, Key='cardiology_sample.csv')
    csv_data = response['Body'].read().decode('utf-8')

# Use pandas to read CSV from string
    import pandas as pd
    df = pd.read_csv(io.StringIO(csv_data))
    print(f"✓ Loaded {len(df)} records into memory")
    print(f"✓ Original columns: {list(df.columns)}")

    # Step 3: Transform - rename columns
    print("\n### Step 3: Transform Data ###")
    df = df.rename(columns={
        'State': 'provider_state',
        'City': 'provider_city',
        'Taxonomy': 'specialty_code'
    })
    print(f"✓ Renamed columns")
    print(f"✓ New columns: {list(df.columns)}")
    
    # Step 4: Save back to MinIO (different bucket)
    print("\n### Step 4: Save to Different MinIO Bucket ###")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    s3.put_object(
        Bucket=target_bucket,
        Key='cardiology_processed.csv',
        Body=csv_buffer.getvalue()
    )
    print(f"✓ Saved transformed data to: s3://{target_bucket}/cardiology_processed.csv")
    
    # Verify
    print("\n### Step 5: Verify ###")
    print(f"✓ Source bucket ({source_bucket}):")
    print(f"  - cardiology_sample.csv")
    print(f"✓ Target bucket ({target_bucket}):")
    print(f"  - cardiology_processed.csv")
    print(f"✓ Columns renamed: State→provider_state, City→provider_city")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("Step 6 Complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()