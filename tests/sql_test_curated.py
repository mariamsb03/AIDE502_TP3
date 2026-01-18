import pymysql
import pymongo
from transformers import AutoTokenizer
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host="localhost",
    user="root",
    password="root",
    database="staging"
)
cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)

# Query all texts from MySQL
cursor.execute("SELECT * FROM texts")

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["curated"]
mongo_collection = mongo_db["wikitext"]

# Load tokenizer
tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")

# Process each row
count = 0
for row in cursor:
    # Tokenize the text
    tokens = tokenizer(
        row["text"], 
        truncation=True, 
        padding=True,
        max_length=128
    )["input_ids"]
    
    # Create document
    document = {
        "id": row["id"],
        "text": row["text"],
        "tokens": tokens,
        "metadata": {
            "source": "mysql",
            "processed_at": datetime.utcnow().isoformat()
        }
    }
    
    # Insert into MongoDB
    mongo_collection.insert_one(document)
    count += 1
    
    if count % 100 == 0:
        print(f"Processed {count} documents...")

print(f"Data successfully inserted into MongoDB collection 'wikitext': {count} documents")

# Close MySQL connection
cursor.close()
mysql_conn.close()
mongo_client.close()