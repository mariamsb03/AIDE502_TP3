import pymysql
import pymongo
from transformers import AutoTokenizer
from datetime import datetime


def process_to_curated(db_host, db_user, db_password, db_name, 
                       mongo_host, mongo_port, mongo_db_name, mongo_collection_name,
                       tokenizer_model="distilbert-base-uncased", batch_size=100):
    """
    Processes data from MySQL staging database, tokenizes texts, and stores in MongoDB curated database.
    
    Steps:
    1. Connects to MySQL staging database
    2. Queries texts directly from the database (no intermediate file)
    3. Tokenizes the texts using a pre-trained NLP tokenizer
    4. Connects to MongoDB
    5. Inserts tokenized data with metadata into MongoDB
    6. Verifies the insertion
    
    Parameters:
    db_host (str): MySQL database host
    db_user (str): MySQL database user
    db_password (str): MySQL database password
    db_name (str): MySQL database name
    mongo_host (str): MongoDB host
    mongo_port (int): MongoDB port
    mongo_db_name (str): MongoDB database name
    mongo_collection_name (str): MongoDB collection name
    tokenizer_model (str): Hugging Face tokenizer model name
    batch_size (int): Number of documents to insert at once
    """
    
    # Step 1: Connect to MySQL database
    print(f"Connecting to MySQL database at {db_host}...")
    try:
        mysql_conn = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        print(f"✓ Connected to MySQL database '{db_name}'")
    except Exception as e:
        print(f"✗ Error connecting to MySQL: {e}")
        raise
    
    # Step 2: Query data from MySQL (direct query, no file!)
    print("\nQuerying data from MySQL staging database...")
    try:
        cursor.execute("SELECT id, text FROM texts WHERE text IS NOT NULL")
        print(f"✓ Query executed successfully")
        
        # Get total count for progress tracking
        cursor.execute("SELECT COUNT(*) as count FROM texts WHERE text IS NOT NULL")
        total_count = cursor.fetchone()['count']
        print(f"✓ Total texts to process: {total_count}")
        
        # Re-execute the main query
        cursor.execute("SELECT id, text FROM texts WHERE text IS NOT NULL")
        
    except Exception as e:
        print(f"✗ Error querying MySQL: {e}")
        mysql_conn.close()
        raise
    
    # Step 3: Load tokenizer
    print(f"\nLoading tokenizer: {tokenizer_model}...")
    try:
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_model)
        print(f"✓ Tokenizer loaded successfully")
    except Exception as e:
        print(f"✗ Error loading tokenizer: {e}")
        mysql_conn.close()
        raise
    
    # Step 4: Connect to MongoDB
    print(f"\nConnecting to MongoDB at {mongo_host}:{mongo_port}...")
    try:
        mongo_client = pymongo.MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        mongo_db = mongo_client[mongo_db_name]
        mongo_collection = mongo_db[mongo_collection_name]
        print(f"✓ Connected to MongoDB database '{mongo_db_name}', collection '{mongo_collection_name}'")
        
        # Clear existing data in collection (optional)
        print(f"\nClearing existing data from collection '{mongo_collection_name}'...")
        result = mongo_collection.delete_many({})
        print(f"✓ Deleted {result.deleted_count} existing documents")
        
    except Exception as e:
        print(f"✗ Error connecting to MongoDB: {e}")
        mysql_conn.close()
        raise
    
    # Step 5: Process and insert data
    print(f"\nProcessing and inserting data into MongoDB...")
    print(f"Batch size: {batch_size} documents\n")
    
    try:
        documents_batch = []
        total_inserted = 0
        
        # Process each row from MySQL
        for row in cursor:
            text_id = row['id']
            text = row['text']
            
            # Tokenize the text
            try:
                tokens = tokenizer(
                    text,
                    truncation=True,
                    padding=True,
                    max_length=128,
                    return_tensors=None  # Return as list, not tensor
                )["input_ids"]
            except Exception as e:
                print(f"⚠ Warning: Failed to tokenize text ID {text_id}: {e}")
                continue
            
            # Create MongoDB document
            document = {
                "id": text_id,
                "text": text,
                "tokens": tokens,
                "metadata": {
                    "source": "mysql",
                    "processed_at": datetime.utcnow().isoformat(),
                    "tokenizer": tokenizer_model,
                    "token_count": len(tokens)
                }
            }
            
            documents_batch.append(document)
            
            # Insert batch when it reaches batch_size
            if len(documents_batch) >= batch_size:
                mongo_collection.insert_many(documents_batch)
                total_inserted += len(documents_batch)
                print(f"  Inserted {total_inserted}/{total_count} documents...", end='\r')
                documents_batch = []
        
        # Insert remaining documents
        if documents_batch:
            mongo_collection.insert_many(documents_batch)
            total_inserted += len(documents_batch)
        
        print(f"\n✓ Successfully inserted {total_inserted} documents into MongoDB")
        
    except Exception as e:
        print(f"\n✗ Error processing data: {e}")
        mysql_conn.close()
        mongo_client.close()
        raise
    
    # Step 6: Verify the insertion
    print("\nVerifying data insertion...")
    try:
        # Count documents in MongoDB
        count = mongo_collection.count_documents({})
        print(f"✓ Total documents in MongoDB: {count}")
        
        # Get average token count
        pipeline = [
            {"$group": {
                "_id": None,
                "avg_tokens": {"$avg": "$metadata.token_count"},
                "min_tokens": {"$min": "$metadata.token_count"},
                "max_tokens": {"$max": "$metadata.token_count"}
            }}
        ]
        stats = list(mongo_collection.aggregate(pipeline))
        if stats:
            print(f"✓ Average tokens per text: {stats[0]['avg_tokens']:.2f}")
            print(f"✓ Token count range: {stats[0]['min_tokens']} - {stats[0]['max_tokens']}")
        
        # Show sample documents
        print("\nSample documents:")
        samples = mongo_collection.find().limit(3)
        for i, doc in enumerate(samples, 1):
            print(f"\n  Sample {i}:")
            print(f"    ID: {doc['id']}")
            print(f"    Text preview: {doc['text'][:80]}...")
            print(f"    Token count: {doc['metadata']['token_count']}")
            print(f"    First 10 tokens: {doc['tokens'][:10]}...")
        
    except Exception as e:
        print(f"✗ Error verifying data: {e}")
    finally:
        cursor.close()
        mysql_conn.close()
        mongo_client.close()
        print("\n✓ All database connections closed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process data from MySQL staging to MongoDB curated")
    parser.add_argument("--db_host", type=str, default="localhost", help="MySQL database host")
    parser.add_argument("--db_user", type=str, default="root", help="MySQL database user")
    parser.add_argument("--db_password", type=str, default="root", help="MySQL database password")
    parser.add_argument("--db_name", type=str, default="staging", help="MySQL database name")
    parser.add_argument("--mongo_host", type=str, default="localhost", help="MongoDB host")
    parser.add_argument("--mongo_port", type=int, default=27017, help="MongoDB port")
    parser.add_argument("--mongo_db_name", type=str, default="curated", help="MongoDB database name")
    parser.add_argument("--mongo_collection_name", type=str, default="wikitext", help="MongoDB collection name")
    parser.add_argument("--tokenizer_model", type=str, default="distilbert-base-uncased", 
                       help="Hugging Face tokenizer model (e.g., 'distilbert-base-uncased' or 'gpt2')")
    parser.add_argument("--batch_size", type=int, default=100, help="Batch size for MongoDB inserts")
    
    args = parser.parse_args()
    
    process_to_curated(
        args.db_host,
        args.db_user,
        args.db_password,
        args.db_name,
        args.mongo_host,
        args.mongo_port,
        args.mongo_db_name,
        args.mongo_collection_name,
        args.tokenizer_model,
        args.batch_size
    )