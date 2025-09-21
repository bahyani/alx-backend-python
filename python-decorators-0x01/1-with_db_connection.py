import sqlite3 
import functools
import logging
from contextlib import contextmanager

# Configure logging for database operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def with_db_connection(func):
    """
    Decorator that automatically handles database connections.
    
    This decorator will:
    1. Open a database connection
    2. Pass the connection as the first argument to the decorated function
    3. Ensure the connection is properly closed after function execution
    4. Handle any exceptions and ensure cleanup
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Default database path - can be customized
        db_path = kwargs.pop('db_path', 'users.db')
        
        conn = None
        try:
            # Open database connection
            conn = sqlite3.connect(db_path)
            logger.info(f"Database connection opened: {db_path}")
            
            # Configure connection for better performance and data handling
            conn.row_factory = sqlite3.Row  # Enable column access by name
            
            # Call the original function with connection as first argument
            result = func(conn, *args, **kwargs)
            
            # Commit any pending transactions
            conn.commit()
            logger.info("Database transaction committed successfully")
            
            return result
            
        except sqlite3.Error as e:
            logger.error(f"Database error occurred: {e}")
            if conn:
                conn.rollback()
                logger.info("Database transaction rolled back")
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            if conn:
                conn.rollback()
                logger.info("Database transaction rolled back")
            raise
            
        finally:
            # Always close the connection
            if conn:
                conn.close()
                logger.info("Database connection closed")
    
    return wrapper

# Enhanced version with configurable database path
def with_db_connection_configurable(db_path='users.db'):
    """
    Configurable decorator that allows specifying the database path.
    
    Usage:
    @with_db_connection_configurable('custom_db.db')
    def my_function(conn, ...):
        ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            conn = None
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                logger.info(f"Database connection opened: {db_path}")
                
                result = func(conn, *args, **kwargs)
                conn.commit()
                
                return result
                
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {e}")
                if conn:
                    conn.rollback()
                raise
                
            finally:
                if conn:
                    conn.close()
                    logger.info("Database connection closed")
        
        return wrapper
    return decorator

# Context manager version for more explicit control
@contextmanager
def db_connection(db_path='users.db'):
    """
    Context manager for database connections.
    
    Usage:
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        logger.info(f"Database connection opened: {db_path}")
        yield conn
        conn.commit()
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
        
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

@with_db_connection 
def get_user_by_id(conn, user_id): 
    cursor = conn.cursor() 
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,)) 
    return cursor.fetchone()

# Additional example functions using the decorator
@with_db_connection
def get_all_users(conn):
    """Fetch all users from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    return cursor.fetchall()

@with_db_connection
def create_user(conn, name, email, age):
    """Create a new user in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        (name, email, age)
    )
    return cursor.lastrowid

@with_db_connection
def update_user(conn, user_id, name=None, email=None, age=None):
    """Update user information."""
    cursor = conn.cursor()
    
    # Build dynamic update query
    updates = []
    params = []
    
    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if email is not None:
        updates.append("email = ?")
        params.append(email)
    if age is not None:
        updates.append("age = ?")
        params.append(age)
    
    if not updates:
        raise ValueError("No fields to update")
    
    params.append(user_id)
    query = f"UPDATE users SET {', '.join(updates)} WHERE id = ?"
    
    cursor.execute(query, params)
    return cursor.rowcount

@with_db_connection
def delete_user(conn, user_id):
    """Delete a user from the database."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
    return cursor.rowcount

@with_db_connection_configurable('test_users.db')
def get_user_from_test_db(conn, user_id):
    """Example using configurable decorator with different database."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    return cursor.fetchone()

# Setup function to create sample database and table
def setup_database():
    """Create sample database and table with test data."""
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Create users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER
            )
        ''')
        
        # Insert sample data
        sample_users = [
            ('John Doe', 'john@example.com', 30),
            ('Jane Smith', 'jane@example.com', 25),
            ('Bob Johnson', 'bob@example.com', 35),
            ('Alice Brown', 'alice@example.com', 28)
        ]
        
        cursor.executemany(
            'INSERT OR IGNORE INTO users (name, email, age) VALUES (?, ?, ?)',
            sample_users
        )
        
        print("Sample database created successfully!")

if __name__ == "__main__":
    # Set up the database with sample data
    setup_database()
    
    # Test the original function
    print("=== Testing Original Function ===")
    user = get_user_by_id(user_id=1)
    if user:
        print(f"User found: ID={user['id']}, Name={user['name']}, Email={user['email']}, Age={user['age']}")
    else:
        print("User not found")
    
    # Test additional functions
    print("\n=== Testing Additional Functions ===")
    
    # Get all users
    all_users = get_all_users()
    print(f"Total users: {len(all_users)}")
    for user in all_users:
        print(f"  - {user['name']} ({user['email']}) - Age: {user['age']}")
    
    # Create a new user
    print("\n--- Creating New User ---")
    new_user_id = create_user("Charlie Wilson", "charlie@example.com", 42)
    print(f"Created user with ID: {new_user_id}")
    
    # Update user
    print("\n--- Updating User ---")
    updated_rows = update_user(new_user_id, age=43)
    print(f"Updated {updated_rows} user(s)")
    
    # Verify update
    updated_user = get_user_by_id(user_id=new_user_id)
    if updated_user:
        print(f"Updated user: {updated_user['name']} - Age: {updated_user['age']}")
    
    # Test error handling
    print("\n=== Testing Error Handling ===")
    try:
        # Try to get non-existent user
        non_existent_user = get_user_by_id(user_id=999)
        print(f"Non-existent user result: {non_existent_user}")
        
        # Try to create user with duplicate email (should fail)
        duplicate_user_id = create_user("Duplicate User", "john@example.com", 25)
        
    except sqlite3.IntegrityError as e:
        print(f"Expected integrity error caught: {e}")
    except Exception as e:
        print(f"Error caught: {e}")
    
    # Clean up - delete the test user
    print("\n--- Cleaning Up ---")
    deleted_rows = delete_user(new_user_id)
    print(f"Deleted {deleted_rows} user(s)")
    
    print("\n=== Testing Completed ===")
    print("The decorator successfully handled all database connections!")
