#!/usr/bin/env python3
"""
Memory-Efficient Aggregation with Generators.
This module provides functions to compute aggregate functions using generators
without loading the entire dataset into memory.
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


def stream_user_ages():
    """
    Generator function that yields user ages one by one from the database.
    Memory-efficient implementation that doesn't load all ages at once.
    
    Yields:
        int: User age
    """
    connection = connect_to_prodev()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()
        
        # Execute query to get only age column
        cursor.execute("SELECT age FROM user_data ORDER BY user_id")
        
        # Loop 1: Fetch ages one by one
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            
            yield int(row[0])
            
    except Exception as e:
        print(f"Error streaming ages: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def calculate_average_age():
    """
    Calculate the average age of all users using the generator.
    Memory-efficient calculation without loading entire dataset.
    
    Returns:
        float: Average age of users
    """
    total_age = 0
    count = 0
    
    # Loop 2: Calculate average using generator
    for age in stream_user_ages():
        total_age += age
        count += 1
    
    if count == 0:
        return 0.0
    
    return total_age / count


# Main execution
if __name__ == "__main__":
    try:
        average_age = calculate_average_age()
        print(f"Average age of users: {average_age}")
    except Exception as e:
        print(f"Error calculating average age: {e}")
    except BrokenPipeError:
        # Handle broken pipe error when output is piped
        sys.stderr.close()
