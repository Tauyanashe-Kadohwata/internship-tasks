import mysql.connector # Use 'import psycopg2' if you are using PostgreSQL

# IMPORTANT: Update these values to match your docker-compose.yml and main.py DB_CONFIG
DB_CONFIG = {
    "host": "127.0.0.1", # Or 'localhost'
    "port": 3306,        # Your MySQL port (or 5432 for PostgreSQL, or 3307/5433 if you changed it)
    "user": "app_user",
    "password": "Scottyest04", # Ensure this matches your docker-compose.yml
    "database": "student_db"
}

try:
    # For MySQL:
    conn = mysql.connector.connect(**DB_CONFIG)
    # For PostgreSQL:
    # conn = psycopg2.connect(**DB_CONFIG)

    if conn.is_connected():
        print("Minimal Test: Database connection successful!")
        conn.close()
    else:
        print("Minimal Test: Database connection failed (but no exception).")
except Exception as e:
    print(f"Minimal Test: An error occurred during connection attempt: {e}")

print("Minimal Test: Script finished.")