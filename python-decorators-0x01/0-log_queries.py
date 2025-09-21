import sqlite3
import functools
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_queries.log'),
        logging.StreamHandler()
    ]
)

#### decorator to log SQL queries

def log_queries(func):
    """
    Decorator that logs SQL queries executed by the decorated function.
    
    This decorator will:
    1. Log the SQL query being executed
    2. Log execution time
    3. Log any errors that occur
    4. Return the original function result
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Extract the query from function arguments
        query = None
        
        # Look for query in positional arguments
        if args:
            for arg in args:
                if isinstance(arg, str) and ('SELECT' in arg.upper() or
                                           'INSERT' in arg.upper() or
                                           'UPDATE' in arg.upper() or
                                           'DELETE' in arg.upper()):
                    query = arg
                    break
        
        # Look for query in keyword arguments
        if not query and kwargs:
            query = kwargs.get('query') or kwargs.get('sql') or kwargs.get('statement')
        
        # Log the function call and query
        function_name = func.__name__
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logging.info(f"🔍 QUERY EXECUTION START")
        logging.info(f"Function: {function_name}")
        logging.info(f"Timestamp: {timestamp}")
        
        if query:
            # Clean up query for better logging (remove extra whitespace)
            clean_query = ' '.join(query.split())
            logging.info(f"SQL Query: {clean_query}")
        else:
            logging.warning("No SQL query found in function arguments")
        
        # Record start time for execution timing
        start_time = datetime.now()
        
        try:
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Log successful execution
            logging.info(f"✅ Query executed successfully")
            logging.info(f"Execution time: {execution_time:.4f} seconds")
            
            if hasattr(result, '__len__'):
                try:
                    logging.info(f"Records returned: {len(result)}")
                except:
                    logging.info("Result returned (length unknown)")
            
            logging.info(f"🔍 QUERY EXECUTION END\n")
            
            return result
            
        except Exception as e:
            # Calculate execution time even for failed queries
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Log the error
            logging.error(f"❌ Query execution failed")
            logging.error(f"Error: {str(e)}")
            logging.error(f"Execution time: {execution_time:.4f} seconds")
            logging.error(f"🔍 QUERY EXECUTION END\n")
            
            # Re-raise the exception to maintain original function behavior
            raise
    
    return wrapper

# Enhanced version with more detailed logging
def log_queries_detailed(func):
    """
    Enhanced decorator with more detailed logging including parameter binding.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        function_name = func.__name__
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create a detailed log entry
        log_entry = {
            'function': function_name,
            'timestamp': timestamp,
            'args': str(args) if args else 'None',
            'kwargs': str(kwargs) if kwargs else 'None'
        }
        
        # Extract query information
        query = None
        params = None
        
        # Look for query in arguments
        for i, arg in enumerate(args):
            if isinstance(arg, str) and any(keyword in arg.upper()
                                          for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
                query = arg
                # Check if next argument might be parameters
                if i + 1 < len(args):
                    next_arg = args[i + 1]
                    if isinstance(next_arg, (list, tuple, dict)):
                        params = next_arg
                break
        
        if not query:
            query = kwargs.get('query') or kwargs.get('sql')
            params = kwargs.get('params') or kwargs.get('parameters')
        
        # Log detailed information
        logging.info("=" * 50)
        logging.info(f"🔍 DATABASE QUERY LOG")
        logging.info("=" * 50)
        logging.info(f"Function: {function_name}")
        logging.info(f"Timestamp: {timestamp}")
        logging.info(f"Arguments: {log_entry['args']}")
        logging.info(f"Keyword Args: {log_entry['kwargs']}")
        
        if query:
            logging.info(f"SQL Query:")
            # Format query for better readability
            formatted_query = query.strip()
            for line in formatted_query.split('\n'):
                logging.info(f"  {line.strip()}")
        
        if params:
            logging.info(f"Parameters: {params}")
        
        logging.info("-" * 30)
        
        start_time = datetime.now()
        
        try:
            result = func(*args, **kwargs)
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            logging.info(f"✅ SUCCESS")
            logging.info(f"Execution Time: {execution_time:.4f}s")
            
            if result:
                if hasattr(result, '__len__'):
                    logging.info(f"Records Count: {len(result)}")
                if isinstance(result, list) and result:
                    logging.info(f"Sample Record: {result[0] if result else 'None'}")
            
            logging.info("=" * 50 + "\n")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            logging.error(f"❌ FAILED")
            logging.error(f"Error Type: {type(e).__name__}")
            logging.error(f"Error Message: {str(e)}")
            logging.error(f"Execution Time: {execution_time:.4f}s")
            logging.error("=" * 50 + "\n")
            
            raise
    
    return wrapper

@log_queries
def fetch_all_users(query):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

@log_queries_detailed
def fetch_user_by_id(query, user_id):
    """Example function with parameters"""
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result

@log_queries
def insert_user(query, user_data):
    """Example function for INSERT operations"""
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query, user_data)
    conn.commit()
    user_id = cursor.lastrowid
    conn.close()
    return user_id

@log_queries
def update_user(query, user_data):
    """Example function for UPDATE operations"""
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query, user_data)
    conn.commit()
    affected_rows = cursor.rowcount
    conn.close()
    return affected_rows

# Example usage and testing
if __name__ == "__main__":
    # Create a sample database and table for testing
    def setup_test_db():
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
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
            ('Bob Johnson', 'bob@example.com', 35)
        ]
        
        cursor.executemany(
            'INSERT OR IGNORE INTO users (name, email, age) VALUES (?, ?, ?)',
            sample_users
        )
        
        conn.commit()
        conn.close()
        
        logging.info("Test database setup completed")

    # Set up test database
    setup_test_db()
    
    # Test the decorator with various queries
    print("Testing log_queries decorator...")
    
    # Test 1: SELECT query
    users = fetch_all_users(query="SELECT * FROM users")
    print(f"Fetched {len(users)} users")
    
    # Test 2: SELECT with WHERE clause
    specific_user = fetch_user_by_id(
        query="SELECT * FROM users WHERE id = ?",
        user_id=1
    )
    print(f"Specific user: {specific_user}")
    
    # Test 3: INSERT query
    new_user_id = insert_user(
        query="INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        user_data=('Alice Brown', 'alice@example.com', 28)
    )
    print(f"Inserted user with ID: {new_user_id}")
    
    # Test 4: UPDATE query
    affected = update_user(
        query="UPDATE users SET age = ? WHERE id = ?",
        user_data=(29, new_user_id)
    )
    print(f"Updated {affected} rows")
    
    # Test 5: Error handling
    try:
        fetch_all_users(query="SELECT * FROM non_existent_table")
    except Exception as e:
        print(f"Caught expected error: {e}")
    
    print("Testing completed! Check 'database_queries.log' for detailed logs.")
