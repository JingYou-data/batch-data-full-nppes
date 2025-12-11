"""
Test connections to MinIO and PostgreSQL
"""
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import psycopg2

load_dotenv()

def test_minio():
    """Test MinIO connection"""
    print("\n### Testing MinIO Connection ###")
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
            config=Config(signature_version='s3v4')
        )
        
        buckets = s3.list_buckets()
        print(f"✓ MinIO connected successfully")
        print(f"✓ Buckets: {[b['Name'] for b in buckets.get('Buckets', [])]}")
        return True
    except Exception as e:
        print(f"✗ MinIO failed: {e}")
        return False

def test_postgres():
    """Test PostgreSQL connection"""
    print("\n### Testing PostgreSQL Connection ###")
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        print(f"✓ PostgreSQL connected")
        print(f"✓ Version: {version[:60]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"✗ PostgreSQL failed: {e}")
        return False

def main():
    print("=" * 70)
    print("Testing Infrastructure Connections")
    print("=" * 70)
    
    minio_ok = test_minio()
    postgres_ok = test_postgres()
    
    print("\n" + "=" * 70)
    if minio_ok and postgres_ok:
        print("✓ All services ready!")
    else:
        print("⚠️  Some services failed")
    print("=" * 70)

if __name__ == "__main__":
    main()