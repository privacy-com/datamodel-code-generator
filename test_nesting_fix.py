#!/usr/bin/env python3

# Test script to validate the get_module_path fix for avoiding double nesting
import sys
from pathlib import Path

# Add the current directory to Python path to import our modules
sys.path.insert(0, '/Users/johnlee/Lithic-NonIndexed/datamodel-code-generator')

from datamodel_code_generator.model.base import get_module_path, get_module_name

# Test cases focusing on the double nesting issue
test_cases = [
    # Your specific problematic case
    ("/private/var/folders/_j/z1l__l8j31d45161jyz386nr0000gp/T/tmpktg70ta5-management_operation_relationship/ledger_api_generated/generated/some_file.py", "TestModel"),
    # Another variant with just generated
    ("/tmp/some_temp/generated/file.py", "MyModel"),
    # Path with multiple nested structure
    ("/tmp/project/ledger_api_generated/generated/subfolder/deep_file.py", "DeepModel"),
    # Normal relative path
    ("./generated/user.py", "UserModel"),
    # Different output directory
    ("./api/models/user.py", "UserModel"),
]

print("Testing get_module_path fixes (avoiding double nesting):")
print("=" * 70)

for file_path_str, name in test_cases:
    file_path = Path(file_path_str)
    result = get_module_path(name, file_path)
    module_name = get_module_name(name, file_path)
    print(f"Input: {file_path_str}")
    print(f"Name: {name}")
    print(f"Module path parts: {result}")
    print(f"Module name: {module_name}")
    print("-" * 50)

print("\nExpected results:")
print("- ledger_api_generated/generated/ paths should only show the filename")
print("- Other paths should show relative structure starting after output dirs")
print("- No 'generated.filename' that would cause generated/generated/ nesting")
