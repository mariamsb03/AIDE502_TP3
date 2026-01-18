import sqlite3

# Connect to (or create) the database
conn = sqlite3.connect('test.db')
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY,
        value TEXT
    )
''')

# Insert a row into the table
cursor.execute('''
    INSERT INTO test_table (value)
    VALUES ("Hello, SQLite!")
''')

# Commit changes so they are saved
conn.commit()

# Query the table
cursor.execute('SELECT * FROM test_table')
rows = cursor.fetchall()

# Print the results
for row in rows:
    print(row)

# Close the connection
conn.close()
