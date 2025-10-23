#!/usr/bin/env python3

# Test script to validate the get_module_path fix
import sys
from pathlib import Path

# Add the current directory to Python path to import our modules
sys.path.insert(0, '/Users/johnlee/Lithic-NonIndexed/datamodel-code-generator')

from datamodel_code_generator.model.base import get_module_path

# Test cases that simulate the problematic paths
test_cases = [
    # Problematic path similar to the error
    ("/private/var/folders/_j/z1l__l8j31d45161jyz386nr0000gp/T/tmpf10g66xo-unified_transactions/ledger_api_generated/generated/some_file.py", "TestModel"),
    # Normal path
    ("./api/models/user.py", "UserModel"),
    # Path with hyphens
    ("./some-folder/my-file.py", "MyModel"),
    # Path with dots
    ("./some.folder/my.file.py", "MyModel"),
]

print("Testing get_module_path fixes:")
print("=" * 50)

for file_path_str, name in test_cases:
    file_path = Path(file_path_str)
    result = get_module_path(name, file_path)
    print(f"Input: {file_path_str}")
    print(f"Name: {name}")
    print(f"Result: {result}")
    print(f"Module name: {'.'.join(result)}")
    print("-" * 30)

print("Test completed!")
