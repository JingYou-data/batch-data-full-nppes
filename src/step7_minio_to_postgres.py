"""
Step 7: Move data from MinIO to PostgreSQL
"""
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import pandas as pd
import psycopg2
from psycopg2 import sql
import io

load_dotenv()

def main():
    print("=" * 70)
    print("Step 7: MinIO → PostgreSQL")
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
    
    # Setup PostgreSQL connection
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    cursor = conn.cursor()
    
    # Step 1: Read data from MinIO
    print("\n### Step 1: Read from MinIO ###")
    bucket = 'processed-data'
    key = 'cardiology_processed.csv'
    
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_data = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(csv_data))
    
    print(f"✓ Read {len(df)} records from MinIO")
    print(f"✓ Columns: {list(df.columns)}")
    
    # Step 2: Create table in PostgreSQL
    print("\n### Step 2: Create PostgreSQL Table ###")
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cardiology_providers (
        npi VARCHAR(10) PRIMARY KEY,
        provider_state VARCHAR(2),
        provider_city VARCHAR(100),
        specialty_code VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    print("✓ Table 'cardiology_providers' created/verified")
    
    # Step 3: Insert data into PostgreSQL
    print("\n### Step 3: Insert Data ###")
    
    # Clear existing data
    cursor.execute("DELETE FROM cardiology_providers")
    
    # Insert data
    insert_query = """
    INSERT INTO cardiology_providers (npi, provider_state, provider_city, specialty_code)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (npi) DO NOTHING
    """
    
    records = df[['NPI', 'provider_state', 'provider_city', 'specialty_code']].values.tolist()
    cursor.executemany(insert_query, records)
    conn.commit()
    
    print(f"✓ Inserted {len(records)} records")
    
    # Step 4: Verify data in PostgreSQL
    print("\n### Step 4: Verify Data ###")
    
    cursor.execute("SELECT COUNT(*) FROM cardiology_providers")
    count = cursor.fetchone()[0]
    print(f"✓ Total records in PostgreSQL: {count}")
    
    cursor.execute("SELECT * FROM cardiology_providers LIMIT 5")
    sample = cursor.fetchall()
    print("\n✓ Sample records:")
    for row in sample:
        print(f"  NPI: {row[0]}, State: {row[1]}, City: {row[2]}")
    
    # Step 5: Summary statistics
    print("\n### Step 5: Summary Statistics ###")
    
    cursor.execute("""
        SELECT provider_state, COUNT(*) as count
        FROM cardiology_providers
        GROUP BY provider_state
        ORDER BY count DESC
        LIMIT 5
    """)
    
    print("✓ Top 5 states:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} providers")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("Step 7 Complete!")
    print("=" * 70)
    print("✓ Data successfully moved from MinIO to PostgreSQL")
    print("✓ Table: cardiology_providers")

if __name__ == "__main__":
    main()