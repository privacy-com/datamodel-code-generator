#!/usr/bin/env python3

# Test script to validate the get_module_path fix
import sys
from pathlib import Path

# Add the current directory to Python path to import our modules
sys.path.insert(0, '/Users/johnlee/Lithic-NonIndexed/datamodel-code-generator')

from datamodel_code_generator.model.base import get_module_path, get_module_name

# Test cases that simulate the problematic paths
test_cases = [
    # Problematic path that was causing double nesting
    ("/private/var/folders/_j/z1l__l8j31d45161jyz386nr0000gp/T/tmpktg70ta5-management_operation_relationship/ledger_api_generated/generated/some_file.py", "TestModel"),
    # Another temp path variant
    ("/var/folders/_j/z1l__l8j31d45161jyz386nr0000gp/T/tmpf10g66xo-unified_transactions/ledger_api_generated/generated/file.py", "MyModel"),
    # Normal relative path
    ("./generated/user.py", "UserModel"),
    # Path with hyphens
    ("./some-folder/my-file.py", "MyModel"),
]

print("Testing get_module_path fixes (for double nesting):")
print("=" * 60)

for file_path_str, name in test_cases:
    file_path = Path(file_path_str)
    result = get_module_path(name, file_path)
    module_name = get_module_name(name, file_path)
    print(f"Input: {file_path_str}")
    print(f"Name: {name}")
    print(f"Module path parts: {result}")
    print(f"Module name: {module_name}")
    print("-" * 40)

print("\nTest completed!")
print("Note: Should only see the file stem, not directory structure.")
