import boto3
import mysql.connector
from mysql.connector import Error


def preprocess_to_staging(bucket_raw, input_file, db_host, db_user, db_password, db_name):
    """
    Downloads WikiText data from raw S3 bucket, cleans it, and stores it in MySQL database.
    
    Steps:
    1. Downloads the raw text file from the raw bucket
    2. Cleans the data (removes duplicates and empty lines)
    3. Connects to MySQL database
    4. Creates 'texts' table if it doesn't exist
    5. Inserts cleaned data into the table
    6. Verifies the insertion
    
    Parameters:
    bucket_raw (str): Name of the raw S3 bucket
    input_file (str): Name of the input file in the raw bucket
    db_host (str): MySQL database host
    db_user (str): MySQL database user
    db_password (str): MySQL database password
    db_name (str): MySQL database name
    """
    
    # Step 1: Download raw data from S3
    print(f"Downloading {input_file} from bucket {bucket_raw}...")
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
    
    try:
        response = s3.get_object(Bucket=bucket_raw, Key=input_file)
        raw_text = response['Body'].read().decode('utf-8')
        print(f"✓ Downloaded {len(raw_text)} characters from S3")
    except Exception as e:
        print(f"✗ Error downloading from S3: {e}")
        raise
    
    # Step 2: Clean the data
    print("\nCleaning data...")
    lines = raw_text.split('\n')
    print(f"  Total lines: {len(lines)}")
    
    # Remove empty lines and strip whitespace
    cleaned_lines = [line.strip() for line in lines if line.strip()]
    print(f"  After removing empty lines: {len(cleaned_lines)}")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_lines = []
    for line in cleaned_lines:
        if line not in seen:
            seen.add(line)
            unique_lines.append(line)
    
    print(f"  After removing duplicates: {len(unique_lines)}")
    print(f"✓ Cleaned data: {len(unique_lines)} unique non-empty texts")
    
    # Step 3: Connect to MySQL database
    print(f"\nConnecting to MySQL database at {db_host}...")
    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        
        if conn.is_connected():
            print(f"✓ Connected to MySQL database '{db_name}'")
            cursor = conn.cursor()
        else:
            raise Error("Failed to connect to MySQL")
            
    except Error as e:
        print(f"✗ Error connecting to MySQL: {e}")
        raise
    
    # Step 4: Create table if it doesn't exist
    print("\nCreating 'texts' table if it doesn't exist...")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS texts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("✓ Table 'texts' is ready")
    except Error as e:
        print(f"✗ Error creating table: {e}")
        conn.close()
        raise
    
    # Clear existing data (optional - remove if you want to append)
    print("\nClearing existing data from 'texts' table...")
    try:
        cursor.execute("TRUNCATE TABLE texts")
        conn.commit()
        print("✓ Table cleared")
    except Error as e:
        print(f"⚠ Warning: Could not clear table: {e}")
    
    # Step 5: Insert cleaned data into the table
    print(f"\nInserting {len(unique_lines)} texts into database...")
    insert_query = "INSERT INTO texts (text) VALUES (%s)"
    
    try:
        # Insert in batches for better performance
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(unique_lines), batch_size):
            batch = unique_lines[i:i + batch_size]
            batch_data = [(text,) for text in batch]
            cursor.executemany(insert_query, batch_data)
            conn.commit()
            total_inserted += len(batch)
            print(f"  Inserted {total_inserted}/{len(unique_lines)} texts...", end='\r')
        
        print(f"\n✓ Successfully inserted {total_inserted} texts into database")
        
    except Error as e:
        print(f"\n✗ Error inserting data: {e}")
        conn.rollback()
        conn.close()
        raise
    
    # Step 6: Verify the insertion
    print("\nVerifying data insertion...")
    try:
        cursor.execute("SELECT COUNT(*) FROM texts WHERE text IS NOT NULL")
        count = cursor.fetchone()[0]
        print(f"✓ Verification: {count} valid rows in 'texts' table")
        
        # Show a sample
        cursor.execute("SELECT id, LEFT(text, 100) as text_preview FROM texts LIMIT 5")
        samples = cursor.fetchall()
        print("\nSample data:")
        for sample in samples:
            print(f"  ID {sample[0]}: {sample[1]}...")
            
    except Error as e:
        print(f"✗ Error verifying data: {e}")
    finally:
        cursor.close()
        conn.close()
        print("\n✓ MySQL connection closed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Preprocess WikiText data from S3 to MySQL staging")
    parser.add_argument("--bucket_raw", type=str, required=True, help="Name of the raw S3 bucket")
    parser.add_argument("--input_file", type=str, default="combined_raw.txt", help="Name of the input file in raw bucket")
    parser.add_argument("--db_host", type=str, default="localhost", help="MySQL database host")
    parser.add_argument("--db_user", type=str, default="root", help="MySQL database user")
    parser.add_argument("--db_password", type=str, default="root", help="MySQL database password")
    parser.add_argument("--db_name", type=str, default="staging", help="MySQL database name")
    args = parser.parse_args()
    
    preprocess_to_staging(
        args.bucket_raw,
        args.input_file,
        args.db_host,
        args.db_user,
        args.db_password,
        args.db_name
    )