import sqlite3
import pandas as pd
import json
import argparse
from datetime import date
from lxml import etree as ET

class DatabaseManager:
    """A class to manage database connections and operations."""
    def __init__(self, db_name=':memory:'):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def create_schema(self):
        """Creates the rooms and students tables with a foreign key constraint."""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS rooms (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                birthday DATE,
                sex TEXT NOT NULL,
                room_id INTEGER,
                FOREIGN KEY (room_id) REFERENCES rooms(id)
            )
        ''')
        self.conn.commit()

    def execute_query(self, query, params=None):
        """Executes a SQL query and returns the results."""
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        """Closes the database connection."""
        self.conn.close()

class DataLoader:
    """Handles loading data from JSON files into the database."""
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def load_data(self, students_file, rooms_file):
        """Loads rooms and students data from JSON files and inserts into the database."""
        try:
            with open(rooms_file, 'r') as f:
                rooms_data = json.load(f)
            self.db_manager.cursor.executemany(
                'INSERT INTO rooms (id, name) VALUES (?, ?)',
                [(room['id'], room['name']) for room in rooms_data]
            )

            with open(students_file, 'r') as f:
                students_data = json.load(f)
            self.db_manager.cursor.executemany(
                'INSERT INTO students (id, name, birthday, sex, room_id) VALUES (?, ?, ?, ?, ?)',
                [(s['id'], s['name'], s['birthday'].split('T')[0], s['sex'], s['room']) for s in students_data]
            )

            self.db_manager.conn.commit()
            print("Data loaded successfully.")
        except Exception as e:
            print(f"Error loading data: {e}")
            self.db_manager.conn.rollback()

class QueryExecutor:
    """Executes the required analytical queries and returns formatted results."""
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def get_query_results(self):
        """
        Executes all required analytical queries and returns a dictionary of results.
        All "mathematics" are performed at the database level using SQLite functions.
        """
        results = {}

        # 1. List of rooms and number of students
        room_student_count_query = """
            SELECT r.name, COUNT(s.id) as student_count
            FROM rooms r
            LEFT JOIN students s ON r.id = s.room_id
            GROUP BY r.name;
        """
        results['rooms_with_student_count'] = self.db_manager.execute_query(room_student_count_query)

        # 2. 5 rooms with the smallest average student age
        min_avg_age_query = """
            SELECT r.name, AVG(CAST(strftime('%Y', 'now') - strftime('%Y', s.birthday) AS INTEGER)) as avg_age
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.name
            ORDER BY avg_age ASC
            LIMIT 5;
        """
        results['min_avg_age_rooms'] = self.db_manager.execute_query(min_avg_age_query)

        # 3. 5 rooms with the largest age difference
        max_age_diff_query = """
            SELECT r.name, MAX(julianday('now') - julianday(s.birthday)) as age_diff_days
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.name
            ORDER BY age_diff_days DESC
            LIMIT 5;
        """
        results['max_age_diff_rooms'] = self.db_manager.execute_query(max_age_diff_query)

        # 4. Rooms with mixed-gender students
        mixed_gender_rooms_query = """
            SELECT r.name, COUNT(DISTINCT s.sex) as unique_sex_count
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.name
            HAVING unique_sex_count > 1;
        """
        results['mixed_gender_rooms'] = self.db_manager.execute_query(mixed_gender_rooms_query)

        return results

class DataSerializer:
    """Handles serialization of query results to JSON or XML."""
    def to_json(self, data):
        """Converts data to JSON format."""
        formatted_results = {}
        for key, rows in data.items():
            if key == 'rooms_with_student_count':
                formatted_results[key] = [{'room_name': row[0], 'student_count': row[1]} for row in rows]
            elif key == 'min_avg_age_rooms':
                formatted_results[key] = [{'room_name': row[0], 'average_age': round(row[1], 2)} for row in rows]
            elif key == 'max_age_diff_rooms':
                formatted_results[key] = [{'room_name': row[0], 'age_difference_days': round(row[1], 2)} for row in rows]
            elif key == 'mixed_gender_rooms':
                formatted_results[key] = [{'room_name': row[0]} for row in rows]
        return json.dumps(formatted_results, indent=4)

    def to_xml(self, data):
        """Converts data to XML format."""
        root = ET.Element('results')
        for key, rows in data.items():
            query_element = ET.SubElement(root, key.replace('_', '-'))
            for row in rows:
                item = ET.SubElement(query_element, 'item')
                if key == 'rooms_with_student_count':
                    ET.SubElement(item, 'room_name').text = str(row[0])
                    ET.SubElement(item, 'student_count').text = str(row[1])
                elif key == 'min_avg_age_rooms':
                    ET.SubElement(item, 'room_name').text = str(row[0])
                    ET.SubElement(item, 'average_age').text = str(round(row[1], 2))
                elif key == 'max_age_diff_rooms':
                    ET.SubElement(item, 'room_name').text = str(row[0])
                    ET.SubElement(item, 'age_difference_days').text = str(round(row[1], 2))
                elif key == 'mixed_gender_rooms':
                    ET.SubElement(item, 'room_name').text = str(row[0])
        return ET.tostring(root, pretty_print=True, xml_declaration=True, encoding='utf-8')

# The main function to tie everything together and create a CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load student data into a database and run queries.")
    parser.add_argument('--students', required=True, help='Path to the students JSON file')
    parser.add_argument('--rooms', required=True, help='Path to the rooms JSON file')
    parser.add_argument('--format', choices=['json', 'xml'], required=True, help='Output format: json or xml')
    args = parser.parse_args()

    # Create an in-memory SQLite database for demonstration
    db_manager = DatabaseManager()
    data_loader = DataLoader(db_manager)
    query_executor = QueryExecutor(db_manager)
    serializer = DataSerializer()

    try:
        db_manager.create_schema()
        data_loader.load_data(args.students, args.rooms)
        
        # Add indexes for query optimization
        db_manager.cursor.execute("CREATE INDEX idx_students_room_id ON students(room_id);")
        db_manager.cursor.execute("CREATE INDEX idx_students_birthday ON students(birthday);")
        db_manager.cursor.execute("CREATE INDEX idx_students_sex ON students(sex);")
        db_manager.conn.commit()
        print("Indexes created for optimization.")

        results = query_executor.get_query_results()

        if args.format == 'json':
            output = serializer.to_json(results)
        else:
            output = serializer.to_xml(results).decode('utf-8')

        print("\nQuery Results:")
        print(output)

    finally:
        db_manager.close()