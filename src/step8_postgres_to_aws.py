"""
Step 8: Move data from PostgreSQL to AWS S3
"""
import os
from dotenv import load_dotenv
import boto3
import psycopg2
import pandas as pd
import io

load_dotenv()

def main():
    print("=" * 70)
    print("Step 8: PostgreSQL → AWS S3")
    print("=" * 70)
    
    # Setup AWS S3 client (uses AWS profile from .env)
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'))
    s3 = session.client('s3', region_name=os.getenv('AWS_REGION'))
    
    aws_bucket = os.getenv('AWS_BUCKET')
    
    # Step 1: Read data from PostgreSQL
    print("\n### Step 1: Read from PostgreSQL ###")
    
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    
    query = "SELECT * FROM cardiology_providers"
    df = pd.read_sql(query, conn)
    
    print(f"✓ Read {len(df)} records from PostgreSQL")
    print(f"✓ Columns: {list(df.columns)}")
    
    conn.close()
    
    # Step 2: Convert to CSV in memory
    print("\n### Step 2: Prepare Data for Upload ###")
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    print(f"✓ Data prepared ({len(csv_buffer.getvalue())} bytes)")
    
    # Step 3: Upload to AWS S3
    print("\n### Step 3: Upload to AWS S3 ###")
    
    s3_key = 'postgres-backup/cardiology_providers.csv'
    
    s3.put_object(
        Bucket=aws_bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    print(f"✓ Uploaded to: s3://{aws_bucket}/{s3_key}")
    
    # Step 4: Verify upload
    print("\n### Step 4: Verify Upload ###")
    
    response = s3.head_object(Bucket=aws_bucket, Key=s3_key)
    file_size = response['ContentLength']
    
    print(f"✓ File size: {file_size:,} bytes")
    print(f"✓ Last modified: {response['LastModified']}")
    
    # Step 5: List objects in bucket
    print("\n### Step 5: List S3 Bucket Contents ###")
    
    response = s3.list_objects_v2(Bucket=aws_bucket)
    
    if 'Contents' in response:
        print(f"✓ Objects in bucket '{aws_bucket}':")
        for obj in response['Contents']:
            print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    else:
        print("✓ Bucket is empty")
    
    print("\n" + "=" * 70)
    print("Step 8 Complete!")
    print("=" * 70)
    print(f"✓ Data successfully moved from PostgreSQL to AWS S3")
    print(f"✓ Location: s3://{aws_bucket}/{s3_key}")
    print(f"✓ Records: {len(df)}")

if __name__ == "__main__":
    main()
    