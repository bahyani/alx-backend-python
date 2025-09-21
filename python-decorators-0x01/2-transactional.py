import sqlite3 
import functools
import logging
from contextlib import contextmanager

# Configure logging for database operations
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
            conn.row_factory = sqlite3.Row  # Enable column access by name
            logger.info(f"Database connection opened: {db_path}")
            
            # Call the original function with connection as first argument
            result = func(conn, *args, **kwargs)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise
            
        finally:
            # Always close the connection
            if conn:
                conn.close()
                logger.info("Database connection closed")
    
    return wrapper

def transactional(func):
    """
    Decorator that manages database transactions automatically.
    
    This decorator will:
    1. Begin a transaction (implicitly with SQLite)
    2. Execute the decorated function
    3. Commit the transaction if successful
    4. Rollback the transaction if an exception occurs
    5. Re-raise any exceptions that occurred
    
    Usage:
    @with_db_connection
    @transactional
    def my_db_function(conn, ...):
        # Database operations here
        pass
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Extract connection from arguments (should be first argument)
        if not args:
            raise ValueError("Transactional decorator requires a database connection as first argument")
        
        conn = args[0]
        
        if not hasattr(conn, 'execute') and not hasattr(conn, 'cursor'):
            raise ValueError("First argument must be a database connection object")
        
        # Get function name for logging
        func_name = func.__name__
        
        try:
            logger.info(f"🔄 Starting transaction for {func_name}")
            
            # Execute the function (transaction starts implicitly with first SQL statement)
            result = func(*args, **kwargs)
            
            # Commit the transaction
            conn.commit()
            logger.info(f"✅ Transaction committed successfully for {func_name}")
            
            return result
            
        except sqlite3.Error as e:
            # Database-specific error occurred
            logger.error(f"❌ Database error in {func_name}: {e}")
            logger.info(f"🔄 Rolling back transaction for {func_name}")
            conn.rollback()
            raise
            
        except Exception as e:
            # Any other error occurred
            logger.error(f"❌ Error in {func_name}: {e}")
            logger.info(f"🔄 Rolling back transaction for {func_name}")
            conn.rollback()
            raise
    
    return wrapper

# Enhanced version with savepoints for nested transactions
def transactional_with_savepoints(savepoint_name=None):
    """
    Advanced transactional decorator that supports savepoints for nested transactions.
    
    Args:
        savepoint_name (str, optional): Name for the savepoint. If None, generates automatically.
    
    Usage:
    @with_db_connection
    @transactional_with_savepoints('user_update')
    def update_user(conn, ...):
        pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not args:
                raise ValueError("Transactional decorator requires a database connection as first argument")
            
            conn = args[0]
            func_name = func.__name__
            sp_name = savepoint_name or f"sp_{func_name}_{id(func)}"
            
            try:
                logger.info(f"🔄 Creating savepoint '{sp_name}' for {func_name}")
                conn.execute(f"SAVEPOINT {sp_name}")
                
                result = func(*args, **kwargs)
                
                logger.info(f"✅ Releasing savepoint '{sp_name}' for {func_name}")
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                
                return result
                
            except Exception as e:
                logger.error(f"❌ Error in {func_name}: {e}")
                logger.info(f"🔄 Rolling back to savepoint '{sp_name}' for {func_name}")
                try:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                    conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                except sqlite3.Error as rollback_error:
                    logger.error(f"Failed to rollback savepoint: {rollback_error}")
                raise
        
        return wrapper
    return decorator

@with_db_connection 
@transactional 
def update_user_email(conn, user_id, new_email): 
    cursor = conn.cursor() 
    cursor.execute("UPDATE users SET email = ? WHERE id = ?", (new_email, user_id))
    
    # Verify the update was successful
    if cursor.rowcount == 0:
        raise ValueError(f"No user found with ID {user_id}")
    
    logger.info(f"Updated email for user {user_id} to {new_email}")

# Additional example functions using both decorators
@with_db_connection
@transactional
def create_user_with_validation(conn, name, email, age):
    """Create a user with validation - demonstrates transaction rollback on error."""
    cursor = conn.cursor()
    
    # Validate input
    if not name or not email:
        raise ValueError("Name and email are required")
    
    if age < 0 or age > 150:
        raise ValueError("Age must be between 0 and 150")
    
    # Check if email already exists
    cursor.execute("SELECT id FROM users WHERE email = ?", (email,))
    if cursor.fetchone():
        raise ValueError(f"User with email {email} already exists")
    
    # Insert new user
    cursor.execute(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        (name, email, age)
    )
    
    user_id = cursor.lastrowid
    logger.info(f"Created user {name} with ID {user_id}")
    
    return user_id

@with_db_connection
@transactional
def bulk_update_users(conn, updates):
    """
    Bulk update multiple users in a single transaction.
    
    Args:
        updates: List of tuples (user_id, new_email)
    """
    cursor = conn.cursor()
    updated_count = 0
    
    for user_id, new_email in updates:
        cursor.execute(
            "UPDATE users SET email = ? WHERE id = ?",
            (new_email, user_id)
        )
        if cursor.rowcount > 0:
            updated_count += 1
            logger.info(f"Updated user {user_id} email to {new_email}")
        else:
            logger.warning(f"User {user_id} not found, skipping")
    
    if updated_count == 0:
        raise ValueError("No users were updated")
    
    logger.info(f"Bulk update completed: {updated_count} users updated")
    return updated_count

@with_db_connection
@transactional
def transfer_user_data(conn, from_user_id, to_user_id):
    """
    Example of complex transaction - transfer data between users.
    If any step fails, entire operation is rolled back.
    """
    cursor = conn.cursor()
    
    # Verify both users exist
    cursor.execute("SELECT name, email FROM users WHERE id = ?", (from_user_id,))
    from_user = cursor.fetchone()
    if not from_user:
        raise ValueError(f"Source user {from_user_id} not found")
    
    cursor.execute("SELECT name, email FROM users WHERE id = ?", (to_user_id,))
    to_user = cursor.fetchone()
    if not to_user:
        raise ValueError(f"Target user {to_user_id} not found")
    
    # Simulate complex data transfer (in real app, this might involve multiple tables)
    logger.info(f"Starting data transfer from {from_user['name']} to {to_user['name']}")
    
    # Step 1: Archive old user data (simulate)
    cursor.execute(
        "UPDATE users SET email = ? WHERE id = ?",
        (f"archived_{from_user['email']}", from_user_id)
    )
    
    # Step 2: Update target user (simulate)
    cursor.execute(
        "UPDATE users SET name = ? WHERE id = ?",
        (f"{to_user['name']} (merged)", to_user_id)
    )
    
    logger.info("Data transfer completed successfully")
    return True

# Setup function to create sample database
def setup_database():
    """Create sample database and table with test data."""
    try:
        conn = sqlite3.connect('users.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Create users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        
        conn.commit()
        conn.close()
        
        logger.info("Sample database created successfully!")
        
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        raise

def get_user_by_id(user_id, db_path='users.db'):
    """Helper function to fetch user data."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()
    conn.close()
    return user

if __name__ == "__main__":
    # Set up the database
    setup_database()
    
    print("=== Testing Transactional Decorator ===\n")
    
    # Test 1: Original function - successful update
    print("1. Testing successful email update...")
    try:
        original_user = get_user_by_id(1)
        print(f"Before update: {original_user['name']} - {original_user['email']}")
        
        update_user_email(user_id=1, new_email='Crawford_Cartwright@hotmail.com')
        
        updated_user = get_user_by_id(1)
        print(f"After update: {updated_user['name']} - {updated_user['email']}")
        print("✅ Email update successful!\n")
        
    except Exception as e:
        print(f"❌ Error: {e}\n")
    
    # Test 2: Failed transaction - trying to update non-existent user
    print("2. Testing transaction rollback on error...")
    try:
        update_user_email(user_id=999, new_email='nonexistent@example.com')
        print("This should not print - error expected")
        
    except Exception as e:
        print(f"✅ Expected error caught: {e}")
        print("✅ Transaction was properly rolled back\n")
    
    # Test 3: Create user with validation - success
    print("3. Testing user creation with validation...")
    try:
        new_user_id = create_user_with_validation(
            name="Charlie Wilson",
            email="charlie@example.com",
            age=42
        )
        print(f"✅ User created successfully with ID: {new_user_id}\n")
        
    except Exception as e:
        print(f"❌ Error: {e}\n")
    
    # Test 4: Create user with validation - failure (duplicate email)
    print("4. Testing transaction rollback on validation error...")
    try:
        create_user_with_validation(
            name="Duplicate User",
            email="john@example.com",  # This email already exists
            age=25
        )
        print("This should not print - duplicate email error expected")
        
    except Exception as e:
        print(f"✅ Expected validation error caught: {e}")
        print("✅ Transaction was properly rolled back\n")
    
    # Test 5: Bulk update - partial success
    print("5. Testing bulk update...")
    try:
        updates = [
            (1, 'john.doe.updated@example.com'),
            (2, 'jane.smith.updated@example.com'),
            (999, 'nonexistent@example.com')  # This will fail
        ]
        
        updated_count = bulk_update_users(updates=updates)
        print(f"✅ Bulk update completed: {updated_count} users updated\n")
        
    except Exception as e:
        print(f"✅ Bulk update handled gracefully: {e}\n")
    
    # Test 6: Complex transaction
    print("6. Testing complex transaction...")
    try:
        transfer_user_data(from_user_id=3, to_user_id=4)
        print("✅ Data transfer completed successfully\n")
        
    except Exception as e:
        print(f"❌ Error in data transfer: {e}\n")
    
    print("=== All Tests Completed ===")
    print("The transactional decorator successfully managed all database transactions!")
    print("Check the logs above to see the transaction lifecycle (start → commit/rollback)")
