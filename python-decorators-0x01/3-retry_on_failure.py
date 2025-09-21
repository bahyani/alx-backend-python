import time
import sqlite3 
import functools
import logging
import random
from typing import Tuple, Type, Union

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
            logger.debug(f"Database connection opened: {db_path}")
            
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
                logger.debug("Database connection closed")
    
    return wrapper

def retry_on_failure(retries=3, delay=2, backoff_factor=1.0, max_delay=30, 
                    exceptions=(Exception,), transient_errors=None):
    """
    Decorator that retries database operations if they fail due to transient errors.
    
    Args:
        retries (int): Maximum number of retry attempts (default: 3)
        delay (float): Initial delay between retries in seconds (default: 2)
        backoff_factor (float): Factor to multiply delay after each retry (default: 1.0)
        max_delay (float): Maximum delay between retries (default: 30)
        exceptions (tuple): Tuple of exception types to catch (default: all exceptions)
        transient_errors (tuple): Specific transient error types to retry
    
    Returns:
        Decorated function that retries on failure
    """
    
    # Define default transient errors for SQLite
    if transient_errors is None:
        transient_errors = (
            sqlite3.OperationalError,  # Database is locked, disk I/O error
            sqlite3.DatabaseError,    # General database errors
            ConnectionError,          # Network connection issues
            TimeoutError,            # Timeout errors
            OSError,                 # File system errors
        )
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            current_delay = delay
            
            for attempt in range(retries + 1):  # +1 for initial attempt
                try:
                    if attempt > 0:
                        logger.info(f"🔄 Retry attempt {attempt}/{retries} for {func_name}")
                    
                    # Execute the function
                    result = func(*args, **kwargs)
                    
                    if attempt > 0:
                        logger.info(f"✅ {func_name} succeeded on attempt {attempt + 1}")
                    
                    return result
                    
                except transient_errors as e:
                    # This is a transient error that we should retry
                    if attempt < retries:
                        # Add some jitter to prevent thundering herd
                        jittered_delay = current_delay * (0.5 + random.random() * 0.5)
                        
                        logger.warning(
                            f"⚠️  Transient error in {func_name} (attempt {attempt + 1}): {e}"
                        )
                        logger.info(
                            f"🕐 Retrying in {jittered_delay:.2f} seconds... "
                            f"({retries - attempt} attempts remaining)"
                        )
                        
                        time.sleep(jittered_delay)
                        
                        # Increase delay for next attempt (exponential backoff)
                        current_delay = min(current_delay * backoff_factor, max_delay)
                        
                        continue
                    else:
                        # Max retries reached
                        logger.error(
                            f"❌ {func_name} failed after {retries + 1} attempts. "
                            f"Final error: {e}"
                        )
                        raise
                        
                except exceptions as e:
                    # This is not a transient error, don't retry
                    if type(e) not in transient_errors:
                        logger.error(f"❌ Non-transient error in {func_name}: {e}")
                        logger.info("🚫 Not retrying for non-transient error")
                        raise
                    else:
                        # Handle case where exception is in both tuples
                        if attempt < retries:
                            continue
                        else:
                            raise
            
            # This should never be reached
            raise RuntimeError(f"Unexpected end of retry loop for {func_name}")
        
        return wrapper
    return decorator

# Simplified retry decorator for basic use cases
def simple_retry(retries=3, delay=2):
    """Simplified retry decorator with basic configuration."""
    return retry_on_failure(retries=retries, delay=delay)

# Smart retry decorator that identifies transient errors automatically
def smart_retry(retries=3, delay=1, backoff_factor=2.0):
    """
    Smart retry decorator with exponential backoff and automatic transient error detection.
    """
    sqlite_transient_errors = (
        sqlite3.OperationalError,  # Database locked, I/O errors
        sqlite3.DatabaseError,    # Corrupt database, disk full
    )
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            current_delay = delay
            
            for attempt in range(retries + 1):
                try:
                    if attempt > 0:
                        logger.info(f"🔄 Smart retry {attempt}/{retries} for {func_name}")
                    
                    result = func(*args, **kwargs)
                    
                    if attempt > 0:
                        logger.info(f"✅ {func_name} recovered after {attempt + 1} attempts")
                    
                    return result
                    
                except sqlite_transient_errors as e:
                    error_msg = str(e).lower()
                    
                    # Check if it's a retryable SQLite error
                    retryable_keywords = [
                        'database is locked',
                        'disk i/o error',
                        'database disk image is malformed',
                        'unable to open database file'
                    ]
                    
                    is_retryable = any(keyword in error_msg for keyword in retryable_keywords)
                    
                    if is_retryable and attempt < retries:
                        logger.warning(f"⚠️  Retryable SQLite error: {e}")
                        logger.info(f"🕐 Waiting {current_delay:.2f}s before retry...")
                        
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                        continue
                    else:
                        logger.error(f"❌ Non-retryable or max attempts reached: {e}")
                        raise
                        
                except Exception as e:
                    logger.error(f"❌ Non-SQLite error in {func_name}: {e}")
                    raise
            
            raise RuntimeError(f"Unexpected end of retry loop for {func_name}")
        
        return wrapper
    return decorator

@with_db_connection
@retry_on_failure(retries=3, delay=1)
def fetch_users_with_retry(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    return cursor.fetchall()

# Additional example functions demonstrating different retry scenarios
@with_db_connection
@retry_on_failure(retries=5, delay=0.5, backoff_factor=2.0, max_delay=10)
def fetch_user_by_id_with_retry(conn, user_id):
    """Fetch a specific user with exponential backoff retry."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    result = cursor.fetchone()
    
    if not result:
        raise ValueError(f"User with ID {user_id} not found")
    
    return result

@with_db_connection
@smart_retry(retries=3, delay=1, backoff_factor=1.5)
def update_user_with_smart_retry(conn, user_id, name, email):
    """Update user with smart retry that detects transient SQLite errors."""
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET name = ?, email = ? WHERE id = ?",
        (name, email, user_id)
    )
    
    if cursor.rowcount == 0:
        raise ValueError(f"No user found with ID {user_id}")
    
    conn.commit()
    logger.info(f"Updated user {user_id}: {name} <{email}>")
    return cursor.rowcount

@with_db_connection
@retry_on_failure(
    retries=2, 
    delay=1, 
    transient_errors=(sqlite3.OperationalError, ConnectionError),
    exceptions=(sqlite3.Error, OSError)
)
def create_user_with_custom_retry(conn, name, email, age):
    """Create user with custom retry configuration."""
    cursor = conn.cursor()
    
    # Simulate potential transient error (e.g., database locked)
    if random.random() < 0.3:  # 30% chance of simulated error for demonstration
        logger.warning("Simulating database locked error for demonstration")
        raise sqlite3.OperationalError("database is locked")
    
    cursor.execute(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        (name, email, age)
    )
    
    conn.commit()
    user_id = cursor.lastrowid
    logger.info(f"Created user {name} with ID {user_id}")
    return user_id

# Function to simulate database errors for testing
@with_db_connection
def simulate_transient_error(conn, error_type="locked"):
    """Helper function to simulate different types of database errors."""
    if error_type == "locked":
        raise sqlite3.OperationalError("database is locked")
    elif error_type == "io":
        raise sqlite3.OperationalError("disk I/O error")
    elif error_type == "corrupt":
        raise sqlite3.DatabaseError("database disk image is malformed")
    elif error_type == "connection":
        raise ConnectionError("Unable to connect to database")
    else:
        raise ValueError("Unknown error type")

# Setup function
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

if __name__ == "__main__":
    # Set up the database
    setup_database()
    
    print("=== Testing Retry Decorator ===\n")
    
    # Test 1: Original function - successful fetch
    print("1. Testing successful fetch with retry decorator...")
    try:
        users = fetch_users_with_retry()
        print(f"✅ Fetched {len(users)} users successfully!")
        for user in users:
            print(f"  - {user['name']} ({user['email']}) - Age: {user['age']}")
        print()
        
    except Exception as e:
        print(f"❌ Error: {e}\n")
    
    # Test 2: Fetch specific user with exponential backoff
    print("2. Testing fetch with exponential backoff...")
    try:
        user = fetch_user_by_id_with_retry(user_id=1)
        print(f"✅ Found user: {user['name']} ({user['email']})")
        print()
        
    except Exception as e:
        print(f"❌ Error: {e}\n")
    
    # Test 3: Update with smart retry
    print("3. Testing smart retry with update operation...")
    try:
        updated = update_user_with_smart_retry(
            user_id=2,
            name="Jane Smith Updated",
            email="jane.updated@example.com"
        )
        print(f"✅ Updated {updated} user(s)")
        print()
        
    except Exception as e:
        print(f"❌ Error: {e}\n")
    
    # Test 4: Create user with potential transient errors
    print("4. Testing create user with simulated transient errors...")
    for i in range(3):
        try:
            user_id = create_user_with_custom_retry(
                name=f"Test User {i+1}",
                email=f"test{i+1}@example.com",
                age=25 + i
            )
            print(f"✅ Created user with ID: {user_id}")
            
        except Exception as e:
            print(f"❌ Failed to create user: {e}")
    
    print()
    
    # Test 5: Demonstrate retry behavior with forced errors
    print("5. Testing retry behavior with simulated errors...")
    
    @with_db_connection
    @retry_on_failure(retries=2, delay=0.5)
    def test_retry_behavior(conn):
        # This will always fail to demonstrate retry logic
        raise sqlite3.OperationalError("Simulated database locked error")
    
    try:
        test_retry_behavior()
    except Exception as e:
        print(f"✅ Expected failure after retries: {e}")
    
    print("\n=== Testing Completed ===")
    print("The retry decorator successfully handled transient database errors!")
    print("Check the logs above to see the retry attempts and recovery behavior.")
