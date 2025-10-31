#!/usr/bin/env python3
"""Simple test to verify drop_index() works"""

import sys
import tempfile
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

try:
    from jasonisnthappy import Database
    print("✅ Successfully imported jasonisnthappy")
except Exception as e:
    print(f"❌ Failed to import: {e}")
    sys.exit(1)

def test_drop_index():
    """Test the drop_index functionality"""
    # Create temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        try:
            # Open database
            db = Database.open(db_path)
            print("✅ Opened database")

            # Create an index
            db.create_index("users", "age_idx", "age", False)
            print("✅ Created index: age_idx")

            # Drop the index (THIS IS THE NEW FEATURE!)
            db.drop_index("users", "age_idx")
            print("✅ Dropped index: age_idx")

            # Try to drop again - should fail
            try:
                db.drop_index("users", "age_idx")
                print("❌ Should have failed dropping non-existent index")
                return False
            except Exception as e:
                print(f"✅ Correctly got error for non-existent index: {e}")

            db.close()
            print("✅ All tests passed!")
            return True

        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    success = test_drop_index()
    sys.exit(0 if success else 1)
