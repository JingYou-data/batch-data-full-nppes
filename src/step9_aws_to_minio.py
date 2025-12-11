"""
Step 9: Move data from AWS S3 back to MinIO
"""
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config

load_dotenv()

def main():
    print("=" * 70)
    print("Step 9: AWS S3 → MinIO")
    print("=" * 70)
    
    # Setup AWS S3 client
    aws_session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'))
    aws_s3 = aws_session.client('s3', region_name=os.getenv('AWS_REGION'))
    
    # Setup MinIO client
    minio_s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    aws_bucket = os.getenv('AWS_BUCKET')
    minio_bucket = 'aws-backup'
    
    # Step 1: Create MinIO bucket
    print("\n### Step 1: Prepare MinIO Bucket ###")
    try:
        minio_s3.create_bucket(Bucket=minio_bucket)
        print(f"✓ Created bucket: {minio_bucket}")
    except Exception:
        print(f"✓ Bucket exists: {minio_bucket}")
    
    # Step 2: Download from AWS S3
    print("\n### Step 2: Download from AWS S3 ###")
    
    s3_key = 'postgres-backup/cardiology_providers.csv'
    
    response = aws_s3.get_object(Bucket=aws_bucket, Key=s3_key)
    data = response['Body'].read()
    
    print(f"✓ Downloaded from AWS: s3://{aws_bucket}/{s3_key}")
    print(f"✓ Size: {len(data):,} bytes")
    
    # Step 3: Upload to MinIO
    print("\n### Step 3: Upload to MinIO ###")
    
    minio_key = 'from-aws/cardiology_providers.csv'
    
    minio_s3.put_object(
        Bucket=minio_bucket,
        Key=minio_key,
        Body=data,
        ContentType='text/csv'
    )
    
    print(f"✓ Uploaded to MinIO: s3://{minio_bucket}/{minio_key}")
    
    # Step 4: Verify in MinIO
    print("\n### Step 4: Verify in MinIO ###")
    
    response = minio_s3.head_object(Bucket=minio_bucket, Key=minio_key)
    print(f"✓ File size: {response['ContentLength']:,} bytes")
    
    # Step 5: List MinIO buckets and contents
    print("\n### Step 5: MinIO Summary ###")
    
    buckets = minio_s3.list_buckets()
    print(f"✓ MinIO buckets:")
    for bucket in buckets['Buckets']:
        print(f"  - {bucket['Name']}")
    
    response = minio_s3.list_objects_v2(Bucket=minio_bucket)
    if 'Contents' in response:
        print(f"\n✓ Contents of '{minio_bucket}':")
        for obj in response['Contents']:
            print(f"  - {obj['Key']} ({obj['Size']:,} bytes)")
    
    print("\n" + "=" * 70)
    print("Step 9 Complete!")
    print("=" * 70)
    print(f"✓ Data successfully moved from AWS S3 to MinIO")
    print(f"✓ AWS location: s3://{aws_bucket}/{s3_key}")
    print(f"✓ MinIO location: s3://{minio_bucket}/{minio_key}")

if __name__ == "__main__":
    main()