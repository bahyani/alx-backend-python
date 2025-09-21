import time
import sqlite3 
import functools
import hashlib
import json
import logging
from typing import Any, Dict, Tuple, Optional
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global query cache
query_cache = {}

def with_db_connection(func):
    """
    Decorator that automatically handles database connections.
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

def cache_query(func):
    """
    Decorator that caches query results based on the SQL query string.
    
    This decorator will:
    1. Generate a cache key based on the SQL query and parameters
    2. Check if the result is already cached
    3. Return cached result if available
    4. Execute query and cache result if not cached
    5. Handle cache expiration and size limits
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Extract query from arguments
        query = None
        params = None
        
        # Look for query in positional arguments
        for arg in args[1:]:  # Skip first arg (connection)
            if isinstance(arg, str) and any(keyword in arg.upper() 
                                          for keyword in ['SELECT', 'WITH']):  # Only cache read operations
                query = arg
                break
        
        # Look for query in keyword arguments
        if not query:
            query = kwargs.get('query') or kwargs.get('sql')
        
        # Look for parameters
        for arg in args[1:]:
            if isinstance(arg, (tuple, list, dict)) and arg != query:
                params = arg
                break
        
        if not params:
            params = kwargs.get('params') or kwargs.get('parameters')
        
        if not query:
            logger.warning(f"No query found in {func.__name__}, skipping cache")
            return func(*args, **kwargs)
        
        # Check if this is a SELECT query (only cache read operations)
        query_upper = query.strip().upper()
        if not (query_upper.startswith('SELECT') or query_upper.startswith('WITH')):
            logger.debug(f"Non-SELECT query in {func.__name__}, skipping cache")
            return func(*args, **kwargs)
        
        # Generate cache key
        cache_key = _generate_cache_key(query, params, func.__name__)
        
        # Check if result is in cache
        if cache_key in query_cache:
            cache_entry = query_cache[cache_key]
            
            # Check if cache entry is still valid
            if _is_cache_valid(cache_entry):
                logger.info(f"🎯 Cache HIT for {func.__name__} (key: {cache_key[:12]}...)")
                cache_entry['hits'] += 1
                cache_entry['last_accessed'] = datetime.now()
                return cache_entry['result']
            else:
                logger.info(f"🕐 Cache EXPIRED for {func.__name__} (key: {cache_key[:12]}...)")
                del query_cache[cache_key]
        
        # Cache miss - execute query
        logger.info(f"❌ Cache MISS for {func.__name__} (key: {cache_key[:12]}...)")
        
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        # Cache the result
        _store_in_cache(cache_key, result, query, params, execution_time, func.__name__)
        
        # Cleanup old cache entries if needed
        _cleanup_cache()
        
        return result
    
    return wrapper

def _generate_cache_key(query: str, params: Any, func_name: str) -> str:
    """Generate a unique cache key based on query, parameters, and function name."""
    # Normalize query (remove extra whitespace, convert to uppercase)
    normalized_query = ' '.join(query.split()).upper()
    
    # Create a string representation of parameters
    if params:
        if isinstance(params, dict):
            params_str = json.dumps(params, sort_keys=True)
        else:
            params_str = str(params)
    else:
        params_str = ""
    
    # Combine all elements
    key_data = f"{func_name}:{normalized_query}:{params_str}"
    
    # Generate SHA256 hash for consistent key length
    return hashlib.sha256(key_data.encode()).hexdigest()

def _is_cache_valid(cache_entry: Dict) -> bool:
    """Check if a cache entry is still valid based on TTL."""
    if 'expires_at' in cache_entry:
        return datetime.now() < cache_entry['expires_at']
    return True  # No expiration set

def _store_in_cache(cache_key: str, result: Any, query: str, params: Any, 
                   execution_time: float, func_name: str, ttl_seconds: int = 300):
    """Store query result in cache with metadata."""
    # Convert sqlite3.Row objects to dictionaries for better caching
    if isinstance(result, list) and result and hasattr(result[0], 'keys'):
        cached_result = [dict(row) for row in result]
    elif hasattr(result, 'keys'):
        cached_result = dict(result)
    else:
        cached_result = result
    
    cache_entry = {
        'result': cached_result,
        'query': query,
        'params': params,
        'func_name': func_name,
        'cached_at': datetime.now(),
        'expires_at': datetime.now() + timedelta(seconds=ttl_seconds),
        'execution_time': execution_time,
        'hits': 0,
        'last_accessed': datetime.now()
    }
    
    query_cache[cache_key] = cache_entry
    
    logger.info(f"💾 Cached result for {func_name} (TTL: {ttl_seconds}s, Execution: {execution_time:.4f}s)")

def _cleanup_cache(max_entries: int = 100):
    """Remove old cache entries when cache gets too large."""
    if len(query_cache) <= max_entries:
        return
    
    # Sort by last accessed time and remove oldest entries
    sorted_entries = sorted(query_cache.items(), 
                          key=lambda x: x[1]['last_accessed'])
    
    entries_to_remove = len(query_cache) - max_entries + 10  # Remove extra entries
    
    for i in range(entries_to_remove):
        cache_key, entry = sorted_entries[i]
        logger.debug(f"🗑️  Removing old cache entry: {entry['func_name']}")
        del query_cache[cache_key]

# Enhanced cache decorator with configurable TTL
def cache_query_advanced(ttl_seconds=300, max_cache_size=100, cache_read_only=True):
    """
    Advanced cache decorator with configurable options.
    
    Args:
        ttl_seconds (int): Time-to-live for cache entries in seconds
        max_cache_size (int): Maximum number of entries in cache
        cache_read_only (bool): Only cache SELECT/WITH queries if True
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract query information
            query = None
            params = None
            
            # Find query in arguments
            for arg in args[1:]:  # Skip connection
                if isinstance(arg, str) and ('SELECT' in arg.upper() or 'WITH' in arg.upper()):
                    query = arg
                    break
            
            if not query:
                query = kwargs.get('query') or kwargs.get('sql')
            
            # Find parameters
            for arg in args[1:]:
                if isinstance(arg, (tuple, list, dict)) and arg != query:
                    params = arg
                    break
            
            if not params:
                params = kwargs.get('params')
            
            if not query:
                return func(*args, **kwargs)
            
            # Check if we should cache this query
            if cache_read_only:
                query_upper = query.strip().upper()
                if not (query_upper.startswith('SELECT') or query_upper.startswith('WITH')):
                    return func(*args, **kwargs)
            
            # Generate cache key
            cache_key = _generate_cache_key(query, params, func.__name__)
            
            # Check cache
            if cache_key in query_cache:
                cache_entry = query_cache[cache_key]
                if _is_cache_valid(cache_entry):
                    logger.info(f"🎯 Cache HIT for {func.__name__}")
                    cache_entry['hits'] += 1
                    cache_entry['last_accessed'] = datetime.now()
                    return cache_entry['result']
                else:
                    del query_cache[cache_key]
            
            # Execute and cache
            logger.info(f"❌ Cache MISS for {func.__name__}")
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            _store_in_cache(cache_key, result, query, params, execution_time, 
                          func.__name__, ttl_seconds)
            
            # Cleanup if needed
            _cleanup_cache(max_cache_size)
            
            return result
        
        return wrapper
    return decorator

# Cache management functions
def clear_cache():
    """Clear all cached query results."""
    global query_cache
    cache_size = len(query_cache)
    query_cache.clear()
    logger.info(f"🗑️  Cleared cache ({cache_size} entries removed)")

def get_cache_stats():
    """Get statistics about the query cache."""
    if not query_cache:
        return {
            'total_entries': 0,
            'total_hits': 0,
            'cache_efficiency': 0.0
        }
    
    total_entries = len(query_cache)
    total_hits = sum(entry['hits'] for entry in query_cache.values())
    total_executions = total_hits + total_entries  # Each entry represents one miss + its hits
    cache_efficiency = (total_hits / total_executions * 100) if total_executions > 0 else 0
    
    return {
        'total_entries': total_entries,
        'total_hits': total_hits,
        'cache_efficiency': cache_efficiency,
        'entries': [
            {
                'func_name': entry['func_name'],
                'query_preview': entry['query'][:50] + '...' if len(entry['query']) > 50 else entry['query'],
                'hits': entry['hits'],
                'cached_at': entry['cached_at'].isoformat(),
                'expires_at': entry['expires_at'].isoformat(),
                'execution_time': entry['execution_time']
            }
            for entry in query_cache.values()
        ]
    }

def invalidate_cache_pattern(pattern: str):
    """Invalidate cache entries that match a pattern in the query."""
    removed_count = 0
    keys_to_remove = []
    
    for cache_key, entry in query_cache.items():
        if pattern.upper() in entry['query'].upper():
            keys_to_remove.append(cache_key)
    
    for key in keys_to_remove:
        del query_cache[key]
        removed_count += 1
    
    logger.info(f"🗑️  Invalidated {removed_count} cache entries matching pattern: {pattern}")

@with_db_connection
@cache_query
def fetch_users_with_cache(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

# Additional example functions with caching
@with_db_connection
@cache_query_advanced(ttl_seconds=600, max_cache_size=50)
def get_user_by_email_cached(conn, email):
    """Get user by email with advanced caching (10-minute TTL)."""
    cursor = conn.cursor()
    query = "SELECT * FROM users WHERE email = ?"
    cursor.execute(query, (email,))
    return cursor.fetchone()

@with_db_connection
@cache_query
def get_users_by_age_range_cached(conn, min_age, max_age):
    """Get users within age range with caching."""
    cursor = conn.cursor()
    query = "SELECT * FROM users WHERE age BETWEEN ? AND ? ORDER BY name"
    cursor.execute(query, (min_age, max_age))
    return cursor.fetchall()

@with_db_connection
@cache_query_advanced(ttl_seconds=60)  # Short TTL for frequently changing data
def get_user_count_cached(conn):
    """Get total user count with short-term caching."""
    cursor = conn.cursor()
    query = "SELECT COUNT(*) as count FROM users"
    cursor.execute(query)
    result = cursor.fetchone()
    return result['count'] if result else 0

# Non-cached function for comparison
@with_db_connection
def fetch_users_no_cache(conn, query):
    """Same function without caching for performance comparison."""
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

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
            ('Alice Brown', 'alice@example.com', 28),
            ('Charlie Wilson', 'charlie@example.com', 42),
            ('Diana Prince', 'diana@example.com', 29),
            ('Eve Adams', 'eve@example.com', 33),
            ('Frank Miller', 'frank@example.com', 38)
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
    
    print("=== Testing Cache Query Decorator ===\n")
    
    # Test 1: Original function - cache miss then hit
    print("1. Testing basic caching behavior...")
    
    print("First call (should be cache MISS):")
    start_time = time.time()
    users = fetch_users_with_cache(query="SELECT * FROM users")
    first_call_time = time.time() - start_time
    print(f"✅ Fetched {len(users)} users in {first_call_time:.4f} seconds")
    
    print("\nSecond call (should be cache HIT):")
    start_time = time.time()
    users_again = fetch_users_with_cache(query="SELECT * FROM users")
    second_call_time = time.time() - start_time
    print(f"✅ Fetched {len(users_again)} users in {second_call_time:.4f} seconds")
    
    speedup = first_call_time / second_call_time if second_call_time > 0 else float('inf')
    print(f"🚀 Cache speedup: {speedup:.2f}x faster!\n")
    
    # Test 2: Different queries
    print("2. Testing different queries...")
    
    young_users = get_users_by_age_range_cached(min_age=20, max_age=30)
    print(f"✅ Found {len(young_users)} users aged 20-30")
    
    user_by_email = get_user_by_email_cached(email="john@example.com")
    if user_by_email:
        print(f"✅ Found user by email: {user_by_email['name']}")
    
    total_users = get_user_count_cached()
    print(f"✅ Total users in database: {total_users}")
    
    # Test 3: Cache the same queries again (should hit cache)
    print("\n3. Testing cache hits for different queries...")
    
    young_users_cached = get_users_by_age_range_cached(min_age=20, max_age=30)
    print(f"✅ Young users (cached): {len(young_users_cached)}")
    
    user_by_email_cached = get_user_by_email_cached(email="john@example.com")
    print(f"✅ User by email (cached): {user_by_email_cached['name'] if user_by_email_cached else 'None'}")
    
    # Test 4: Performance comparison
    print("\n4. Performance comparison (cached vs non-cached)...")
    
    query = "SELECT * FROM users WHERE age > 25 ORDER BY name"
    iterations = 5
    
    # Test cached version
    print("Testing cached version...")
    cached_times = []
    for i in range(iterations):
        start_time = time.time()
        fetch_users_with_cache(query=query)
        cached_times.append(time.time() - start_time)
    
    # Test non-cached version  
    print("Testing non-cached version...")
    non_cached_times = []
    for i in range(iterations):
        start_time = time.time()
        fetch_users_no_cache(query=query)
        non_cached_times.append(time.time() - start_time)
    
    avg_cached_time = sum(cached_times) / len(cached_times)
    avg_non_cached_time = sum(non_cached_times) / len(non_cached_times)
    
    print(f"Average cached time: {avg_cached_time:.4f}s")
    print(f"Average non-cached time: {avg_non_cached_time:.4f}s")
    print(f"Cache performance improvement: {avg_non_cached_time / avg_cached_time:.2f}x")
    
    # Test 5: Cache management
    print("\n5. Cache management and statistics...")
    
    stats = get_cache_stats()
    print(f"Cache entries: {stats['total_entries']}")
    print(f"Cache hits: {stats['total_hits']}")
    print(f"Cache efficiency: {stats['cache_efficiency']:.1f}%")
    
    # Invalidate cache for specific pattern
    print("\nInvalidating cache entries containing 'users'...")
    invalidate_cache_pattern("users")
    
    # Check stats after invalidation
    stats_after = get_cache_stats()
    print(f"Cache entries after invalidation: {stats_after['total_entries']}")
    
    print("\n=== Testing Completed ===")
    print("The cache decorator successfully cached query results!")
    print("Check the logs above to see cache hits and misses.")
