import mysql.connector

# Connect to MySQL database
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="root",
    database="staging"
)
cursor = conn.cursor()

# Check data
cursor.execute("SELECT COUNT(*) FROM texts WHERE text IS NOT NULL")
count = cursor.fetchone()
print(f"Number of valid rows: {count[0]}")

cursor.close()
conn.close()