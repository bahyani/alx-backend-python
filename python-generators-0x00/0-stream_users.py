#!/usr/bin/env python3
"""
Generator that streams rows from an SQL database one by one.
This module provides a function to fetch users from the database using a generator.
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
        print("❌ Error: mysql-connector-python not installed.")
        print("Please install it using: pip install mysql-connector-python")
        return None
    except Exception as e:
        print(f"❌ Error connecting to ALX_prodev database: {e}")
        return None


def stream_users():
    """
    Generator function that streams rows from the user_data table one by one.
    Uses yield to provide memory-efficient iteration over database records.
    
    Yields:
        dict: User record as dictionary containing user_id, name, email, age, 
              created_at, and updated_at
    """
    # Establish database connection
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
        
        # Single loop to yield rows one by one
        while True:
            row = cursor.fetchone()
            if row is None:
                break
                
            # Convert row tuple to dictionary for easier access
            user_dict = {
                'user_id': row[0],
                'name': row[1],
                'email': row[2],
                'age': int(row[3]),
                'created_at': row[4],
                'updated_at': row[5]
            }
            
            yield user_dict
            
    except Exception as e:
        print(f"❌ Error streaming data: {e}")
    finally:
        # Ensure proper cleanup of database resources
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# Example usage for testing
if __name__ == "__main__":
    print("🔄 Testing stream_users generator...")
    
    user_count = 0
    for user in stream_users():
        if user_count < 10:  # Show first 10 users
            print(user)
            user_count += 1
        else:
            break
    
    print(f"\n📊 Displayed first {user_count} users from the generator.")
