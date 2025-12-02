# 0x03. Unit Tests and Integration Tests

## Project Overview
This project focuses on **unit testing** and **integration testing** in Python. Unit tests ensure individual functions behave as expected, while integration tests verify end-to-end code paths, interactions between components, and low-level function calls.  

We explore testing patterns including **mocking**, **parametrization**, and **fixtures** to write robust, maintainable tests.

---

## Learning Objectives
By the end of this project, you should be able to:
- Explain the difference between **unit** and **integration** tests.
- Use **mocking** to isolate unit tests from external dependencies.
- Apply **parameterized tests** for multiple input scenarios.
- Understand the role of **fixtures** in test setup and teardown.

---

## Requirements
- Python 3.7+ on Ubuntu 18.04 LTS
- All files must be executable and follow **pycodestyle v2.5** style
- Modules, classes, and functions must include proper docstrings.
- Unit and integration tests must be **type-annotated**.

---

## Project Files
- `utils.py` – Contains utility functions for GitHub client:
  - `access_nested_map(nested_map: Mapping, path: Sequence) -> Any`
  - `get_json(url: str) -> Dict`
  - `memoize(fn: Callable) -> Callable`
- `client.py` – Handles HTTP requests (can be mocked in tests)
- `fixtures.py` – Provides test fixtures
- `test_utils.py` – Contains unit tests for `utils.py`

---

## Example Unit Test
```python
from parameterized import parameterized
import unittest
from utils import access_nested_map

class TestAccessNestedMap(unittest.TestCase):
    @parameterized.expand([
        ({"a": 1}, ("a",), 1),
        ({"a": {"b": 2}}, ("a",), {"b": 2}),
        ({"a": {"b": 2}}, ("a", "b"), 2),
    ])
    def test_access_nested_map(self, nested_map, path, expected):
        self.assertEqual(access_nested_map(nested_map, path), expected)

