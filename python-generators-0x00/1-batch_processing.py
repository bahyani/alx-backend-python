#!/usr/bin/env python3
"""
Batch processing of large data using generators.
This module provides functions to fetch and process data in batches from the users database.
"""

import sys
import os


def connect_to_prodev():
    """
    Connects to the ALX_prodev database in MySQL.
    
    Returns:
        Connection object if successful, None otherwise
    """
    try:
        import mysql.connector
        
        connection = mysql.connector.connect(
            host='localhost',
            user='root',  # Change as needed
            password='',  # Add your MySQL password here
            database='ALX_prodev',
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        
        if connection.is_connected():
            return connection
            
    except ImportError:
        print("Error: mysql-connector-python not installed.")
        print("Please install it using: pip install mysql-connector-python")
        return None
    except Exception as e:
        print(f"Error connecting to ALX_prodev database: {e}")
        return None


def stream_users_in_batches(batch_size):
    """
    Generator function that fetches rows in batches from the user_data table.
    
    Args:
        batch_size (int): Number of rows to fetch in each batch
        
    Yields:
        list: Batch of user records as list of dictionaries
    """
    connection = connect_to_prodev()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()
        
        # Execute query to get all users ordered by user_id for consistent results
        cursor.execute("""
            SELECT user_id, name, email, age, created_at, updated_at 
            FROM user_data 
            ORDER BY user_id
        """)
        
        # Loop 1: Fetch data in batches
        while True:
            # Fetch batch_size number of rows
            rows = cursor.fetchmany(batch_size)
            
            if not rows:
                break
            
            # Convert batch of rows to list of dictionaries
            batch = []
            
            # Loop 2: Process each row in the current batch
            for row in rows:
                user_dict = {
                    'user_id': row[0],
                    'name': row[1],
                    'email': row[2],
                    'age': int(row[3]),
                    'created_at': row[4],
                    'updated_at': row[5]
                }
                batch.append(user_dict)
            
            yield batch
            
    except Exception as e:
        print(f"Error streaming data in batches: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def batch_processing(batch_size):
    """
    Processes each batch to filter users over the age of 25.
    
    Args:
        batch_size (int): Number of rows to process in each batch
    """
    # Loop 3: Process each batch
    for batch in stream_users_in_batches(batch_size):
        # Filter users over age 25 and print them
        for user in batch:
            if user['age'] > 25:
                print(user)
                print()  # Add blank line for readability


# Example usage for testing
if __name__ == "__main__":
    print("Testing batch processing with batch size 50...")
    batch_processing(50)
