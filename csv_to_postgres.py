import os
import time
import pandas as pd
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "anki1"
DB_USER = "postgres"
DB_PASS = "Ankit@1692"

# Folder to monitor
FOLDER_PATH = r"C:\Users\z0052w0z\Desktop\csvfiles"

# Store file modification times
file_timestamps = {}

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def create_or_update_table(file_path, conn):
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # Read CSV
    df = pd.read_csv(file_path, low_memory=False, encoding='utf-8')

    # Remove empty rows
    df.dropna(how="all", inplace=True)

    # Convert all columns to strings
    df = df.astype(str)

    if "TestLogFolder" not in df.columns:
        print(f"Skipping '{file_path}' because 'TestLogFolder' column is missing.")
        return

    with conn.cursor() as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table_name,))
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            # Creating table with TestLogFolder as PRIMARY KEY
            columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
            cur.execute(sql.SQL("""
                CREATE TABLE {} ({}, PRIMARY KEY ("TestLogFolder"));
            """).format(sql.Identifier(table_name), sql.SQL(columns)))
            
            print(f"Table '{table_name}' created with PRIMARY KEY 'TestLogFolder'.")
        else:
            # Add new columns if they appear in the CSV
            cur.execute("""
                SELECT column_name FROM information_schema.columns WHERE table_name = %s;
            """, (table_name,))
            existing_columns = {row[0] for row in cur.fetchall()}
            new_columns = set(df.columns) - existing_columns
            
            for col in new_columns:
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT;").format(
                    sql.Identifier(table_name), sql.Identifier(col)))
                print(f"New column '{col}' added to table '{table_name}'.")

        # Insert data while avoiding duplicates
        for _, row in df.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            columns = ", ".join([f'"{col}"' for col in df.columns])
            values = tuple(row)

            insert_query = sql.SQL("""
                INSERT INTO {} ({}) VALUES ({}) 
                ON CONFLICT ("TestLogFolder") DO NOTHING;
            """).format(sql.Identifier(table_name), sql.SQL(columns), sql.SQL(placeholders))
            
            try:
                cur.execute(insert_query, values)
            except Exception as e:
                print(f"Error inserting row: {e}")

        print(f"Data from '{file_path}' inserted/updated in table '{table_name}'.")
    
    conn.commit()

def monitor_folder():
    global file_timestamps
    while True:
        csv_files = [f for f in os.listdir(FOLDER_PATH) if f.endswith(".csv")]
        
        if not csv_files:
            print("No CSV files found in the folder.")
        else:
            conn = get_connection()
            for file in csv_files:
                file_path = os.path.join(FOLDER_PATH, file)
                modified_time = os.path.getmtime(file_path)
                
                if file not in file_timestamps or file_timestamps[file] != modified_time:
                    print(f"Processing file: {file}")
                    create_or_update_table(file_path, conn)
                    file_timestamps[file] = modified_time
            conn.close()
        
        time.sleep(20)

if __name__ == "__main__":
    monitor_folder()
