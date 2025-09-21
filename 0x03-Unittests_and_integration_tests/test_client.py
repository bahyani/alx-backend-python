
#!/usr/bin/env python3
"""Utility functions module."""

import functools
import requests
from typing import Dict, Tuple, Any, Union, Callable


def access_nested_map(nested_map: Dict, path: Tuple[str]) -> Any:
    """
    Access a nested map using a sequence of keys.
    
    Args:
        nested_map: A nested dictionary
        path: A tuple of keys representing the path to the desired value
        
    Returns:
        The value at the specified path in the nested map
        
    Raises:
        KeyError: If any key in the path doesn't exist
    """
    current = nested_map
    
    for key in path:
        if not isinstance(current, dict) or key not in current:
            raise KeyError(key)
        current = current[key]
    
    return current


def get_json(url: str) -> Dict:
    """
    Get JSON data from a URL.
    
    Args:
        url: The URL to fetch JSON data from
        
    Returns:
        Dictionary containing the JSON response
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def memoize(func: Callable) -> property:
    """
    Decorator that turns methods into memoized properties.
    
    This decorator caches the result of a method call and returns
    the cached result on subsequent accesses, effectively turning
    the method into a property that is only computed once.
    
    Args:
        func: The method to be memoized
        
    Returns:
        A property that caches the method's result
        
    Example:
        class MyClass:
            @memoize
            @property  # Note: @property should come after @memoize
            def expensive_operation(self):
                # This will only be called once
                return some_expensive_computation()
                
        # Usage:
        obj = MyClass()
        result1 = obj.expensive_operation  # Calls the method
        result2 = obj.expensive_operation  # Returns cached result
    """
    attr_name = f'_{func.__name__}'
    
    @functools.wraps(func)
    def wrapper(self):
        # Check if we already have a cached result
        if not hasattr(self, attr_name):
            # Cache the result
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)
    
    return property(wrapper)
