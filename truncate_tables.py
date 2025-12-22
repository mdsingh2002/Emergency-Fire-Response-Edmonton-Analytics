"""
Truncate all tables to allow fresh data load
"""
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

cur = conn.cursor()

print("Truncating tables...")
try:
    cur.execute("TRUNCATE TABLE fire_incidents, dim_event_types, dim_response_codes, dim_neighbourhoods CASCADE;")
    conn.commit()
    print("Tables truncated successfully!")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    cur.close()
    conn.close()
