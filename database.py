from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import os
import sqlite3
from datetime import datetime


def get_db_connection():
    """
    Create and return a connection to the SQLite database
    """
    conn = sqlite3.connect("dengue.db")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """
    Initialize the SQLite database and create tables if they don't exist
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create the dengue_data table if it doesn't exist
    # We'll use a dynamic approach based on the CSV structure
    file_path = os.path.join(os.getcwd(), "dengue-dataset.csv")

    if os.path.exists(file_path):
        df = pd.read_csv(file_path, nrows=1)
        columns = df.columns.tolist()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dengue_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
        """

        # Add each column from the CSV
        for col in columns:
            # Sanitize column name for SQLite
            safe_col = col.replace(" ", "_").replace("-", "_")
            create_table_sql += f'"{safe_col}" TEXT,'

        # Add timestamp and close statement
        create_table_sql += """
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_table_sql)
    else:
        # Fallback generic table structure
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS dengue_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

    conn.commit()
    conn.close()

    return "Database initialized"


def save_dengue_data_to_db(data):
    """
    Save dengue data to the SQLite database

    Args:
        data (list): List of dictionaries containing dengue data
    """
    if not data:
        return {"message": "No data to save"}

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get columns from the first data item
    columns = data[0].keys()
    # Sanitize column names
    columns = [col.replace(" ", "_").replace("-", "_") for col in columns]
    columns_str = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join(["?"] * len(columns))

    # Insert each row of data
    records_added = 0
    for row in data:
        values = [
            row.get(col.replace("_", " ").replace("_", "-"), None) for col in columns
        ]
        try:
            cursor.execute(
                f"INSERT INTO dengue_data ({columns_str}) VALUES ({placeholders})",
                values,
            )
            records_added += 1
        except sqlite3.Error as e:
            print(f"Error inserting row: {e}")

    conn.commit()
    conn.close()

    return {"message": f"Saved {records_added} records to database"}
