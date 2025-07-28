import json
import os
import datetime

# Import the appropriate database connector
# For MySQL:
import mysql.connector
# For PostgreSQL:
# import psycopg2


class DatabaseManager:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        # Implement database connection logic here
        # Use self.host, self.port, etc.
        # Example for MySQL:
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Database connected successfully.")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise # Re-raise the exception to stop execution if connection fails

    def execute_query(self, query, params=None):
        # Implement query execution logic here
        # Make sure to handle INSERTs with many rows efficiently (executemany if possible)
        # Example:
        cursor = None
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.executemany(query, params) # Use executemany for multiple rows
            else:
                cursor.execute(query)
            self.connection.commit()
            return cursor
        except Exception as e:
            self.connection.rollback() # Rollback on error
            print(f"Error executing query: {e}")
            raise # Re-raise the exception
        finally:
            if cursor:
                cursor.close()

    def fetch_all(self, query, params=None):
        # Implement logic to fetch all results from a SELECT query
        cursor = None
        try:
            cursor = self.connection.cursor(dictionary=True) # or use cursor.fetchall() and then dict comprehension
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise
        finally:
            if cursor:
                cursor.close()


    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed.")


class DataLoader:
    def __init__(self):
        pass # No specific instance data needed for now, methods will take file paths

    def load_json(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error decoding JSON from {file_path}: {e}")
        except Exception as e:
            raise IOError(f"Error reading file {file_path}: {e}")

    # You might want specific methods like these, or just use load_json directly
    def load_rooms(self, file_path):
        return self.load_json(file_path)

    def load_students(self, file_path):
        return self.load_json(file_path)


# Main execution block
if __name__ == "__main__":
    # This is where you'll put your main logic to:
    # 1. Define database credentials (from your docker-compose.yml)
    # 2. Instantiate DatabaseManager and DataLoader
    # 3. Call methods to load data
    # 4. Process student data to calculate age
    # 5. Insert data into the database (rooms first, then students)
    # 6. Execute required queries and print results (later steps)

    # Example place-holders (REPLACE WITH YOUR ACTUAL CREDENTIALS AND FILE PATHS)
    DB_CONFIG = {
        "host": "127.0.0.1", # Or 'localhost'
        "port": 3306,        # Your MySQL port (or 5432 for PostgreSQL)
        "user": "app_user",
        "password": "Scottyest04",
        "database": "student_db"
    }

    ROOMS_FILE = "data/rooms.json"
    STUDENTS_FILE = "data/students.json"

    db_manager = None
    try:
        db_manager = DatabaseManager(**DB_CONFIG)
        db_manager.connect()

        data_loader = DataLoader()

        # Load rooms data
        print(f"Loading rooms from {ROOMS_FILE}...")
        rooms_data = data_loader.load_rooms(ROOMS_FILE)
        print(f"Loaded {len(rooms_data)} rooms.")

        # Prepare rooms for insertion
        rooms_to_insert = [(room['id'], room['name']) for room in rooms_data]
        print(f"Inserting {len(rooms_to_insert)} rooms into database...")
        # Note: For efficiency, we collect all rooms and insert them in one batch using executemany
        # Ensure your DB connector's executemany supports this tuple format for values
        db_manager.execute_query("INSERT INTO rooms (id, name) VALUES (%s, %s)", rooms_to_insert)
        print("Rooms inserted successfully.")


        # Load students data
        print(f"Loading students from {STUDENTS_FILE}...")
        students_raw_data = data_loader.load_students(STUDENTS_FILE)
        print(f"Loaded {len(students_raw_data)} students.")

        # Process students data (calculate age) and prepare for insertion
        students_to_insert = []
        current_date = datetime.date.today()
        for student in students_raw_data:
            student_id = student['id']
            student_name = student['name']
            birthday_str = student['birthday'].split('T')[0] # "YYYY-MM-DDTHH:MM:SS.microseconds" -> "YYYY-MM-DD"
            sex = student['sex']
            room_id = student['room']

            # Calculate age from birthday
            # Handle potential variations in 'sex' field values if necessary (e.g., 'M'/'F' vs 'male'/'female')
            # Assuming 'M' or 'F' as per your provided JSON content, already VARCHAR(1) in schema.

            try:
                birthday_date = datetime.datetime.strptime(birthday_str, '%Y-%m-%d').date()
                age = current_date.year - birthday_date.year - ((current_date.month, current_date.day) < (birthday_date.month, birthday_date.day))
            except ValueError as ve:
                print(f"Warning: Could not parse birthday for student ID {student_id} ({birthday_str}). Skipping age calculation for this student. Error: {ve}")
                age = None # Or a default value, or skip insertion

            if age is not None:
                students_to_insert.append((student_id, student_name, birthday_date, sex, age, room_id))

        print(f"Prepared {len(students_to_insert)} students for insertion (with age calculated).")

        # Insert students data
        # IMPORTANT: Students must be inserted AFTER rooms due to foreign key constraint
        db_manager.execute_query(
            "INSERT INTO students (id, name, birthday, sex, age, room_id) VALUES (%s, %s, %s, %s, %s, %s)",
            students_to_insert
        )
        print("Students inserted successfully.")


    except FileNotFoundError as e:
        print(f"File Error: {e}")
    except ValueError as e:
        print(f"Data Error: {e}")
    except IOError as e:
        print(f"IO Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if db_manager:
            db_manager.close()