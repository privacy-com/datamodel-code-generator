#!/usr/bin/env python3
"""
Test script to reproduce the allOf external reference bug described by the user.

The issue: when using allOf with external references like:
{
    "allOf": [
        {
            "$ref": "./unified_base_transaction.json"
        }
    ]
}

The import should be:
from .unified_base_transaction import UnifiedBaseTransaction

But it's generating:
from .UnifiedBaseTransaction import UnifiedBaseTransaction
"""

import tempfile
import json
from pathlib import Path
import sys

# Add the current directory to Python path to import our modules
sys.path.insert(0, '/Users/johnlee/Lithic-NonIndexed/datamodel-code-generator')

from datamodel_code_generator import generate

def create_test_schemas():
    """Create test schemas that reproduce the bug"""

    # Create unified_base_transaction.json
    unified_base_transaction = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://api.lithic.com/v1/schemas/treasury/ledger/unified_base_transaction.json",
        "title": "unified_base_transaction",
        "description": "Base class for all transaction types in the ledger service",
        "type": "object",
        "properties": {
            "family": {
                "type": "string",
                "description": "The family of the transaction",
                "enum": [
                    "CARD",
                    "PAYMENT",
                    "TRANSFER",
                    "FINANCIAL",
                    "BOOK_TRANSFER",
                    "EXTERNAL_PAYMENT",
                    "MANAGEMENT_OPERATION"
                ]
            },
            "status": {
                "$ref": "./transaction_status.json",
                "description": "The status of the transaction"
            },
            "token": {
                "type": "string",
                "description": "Unique identifier for the transaction",
                "format": "uuid"
            },
            "created": {
                "type": "string",
                "description": "ISO 8601 timestamp of when the transaction was created",
                "format": "date-time"
            },
            "updated": {
                "type": "string",
                "description": "ISO 8601 timestamp of when the transaction was last updated",
                "format": "date-time"
            },
            "amount": {
                "type": "integer",
                "description": "The amount of the transaction in cents"
            },
            "currency": {
                "type": "string",
                "description": "The currency of the transaction",
                "default": "USD"
            }
        },
        "required": [
            "family",
            "status",
            "token",
            "created",
            "updated",
            "amount",
            "currency"
        ]
    }

    # Create financial_transaction.json with allOf reference
    financial_transaction = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://api.lithic.com/v1/schemas/treasury/ledger/financial_transaction.json",
        "title": "FinancialTransaction",
        "description": "Financial transaction with inheritance from unified base transaction",
        "allOf": [
            {
                "$ref": "./unified_base_transaction.json"
            },
            {
                "type": "object",
                "properties": {
                    "specific_field": {
                        "type": "string",
                        "description": "A field specific to financial transactions"
                    }
                }
            }
        ]
    }

    # Create transaction_status.json (referenced by unified_base_transaction)
    transaction_status = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://api.lithic.com/v1/schemas/treasury/ledger/transaction_status.json",
        "title": "TransactionStatus",
        "type": "string",
        "enum": ["PENDING", "COMPLETED", "FAILED"]
    }

    return unified_base_transaction, financial_transaction, transaction_status

def test_allof_external_reference_bug():
    """Test the allOf external reference import bug"""

    unified_base_transaction, financial_transaction, transaction_status = create_test_schemas()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Write the schema files
        (temp_path / "unified_base_transaction.json").write_text(
            json.dumps(unified_base_transaction, indent=2)
        )
        (temp_path / "financial_transaction.json").write_text(
            json.dumps(financial_transaction, indent=2)
        )
        (temp_path / "transaction_status.json").write_text(
            json.dumps(transaction_status, indent=2)
        )

        # Generate the code
        output_dir = temp_path / "output"
        output_dir.mkdir()

        try:
            # Use the command-line interface instead
            import subprocess
            import os

            # Change to temp directory first
            original_cwd = os.getcwd()
            os.chdir(temp_path)

            try:
                result = subprocess.run([
                    sys.executable, "-c",
                    "import sys; sys.path.insert(0, '/Users/johnlee/Lithic-NonIndexed/datamodel-code-generator'); from datamodel_code_generator.__main__ import main; main()",
                    "--input", "financial_transaction.json",
                    "--output", str(output_dir),
                    "--input-file-type", "jsonschema"
                ], capture_output=True, text=True, cwd=temp_path)

                if result.returncode != 0:
                    print(f"Error running generator: {result.stderr}")
                    return

            finally:
                os.chdir(original_cwd)

            # Check the generated files
            generated_files = list(output_dir.rglob("*.py"))
            print("Generated files:")
            for file in generated_files:
                print(f"  {file.relative_to(output_dir)}")
                print("Content:")
                content = file.read_text()
                print(content)
                print("-" * 80)

                # Check for the problematic import
                if "from .UnifiedBaseTransaction import" in content:
                    print("❌ BUG FOUND: Using class name as module name in import!")
                    print("   Should be: from .unified_base_transaction import UnifiedBaseTransaction")
                elif "from .unified_base_transaction import" in content:
                    print("✅ CORRECT: Using original filename as module name")

        except Exception as e:
            print(f"Error generating code: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_allof_external_reference_bug()
