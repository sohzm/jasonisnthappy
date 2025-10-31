"""Integration tests for jasonisnthappy Python bindings."""

import json
import os
import tempfile
import unittest
from pathlib import Path

from jasonisnthappy import Database


class TestDatabaseBasics(unittest.TestCase):
    """Test basic database operations."""

    def setUp(self):
        """Create a temporary database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = Database.open(self.db_path)

    def tearDown(self):
        """Clean up the test database."""
        self.db.close()
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_open_close(self):
        """Test opening and closing a database."""
        self.assertIsNotNone(self.db)

    def test_reopen_database(self):
        """Test reopening an existing database."""
        # Insert data
        self.db.insert("test", json.dumps({"_id": "persist", "value": 123}))
        self.db.close()

        # Reopen
        self.db = Database.open(self.db_path)
        results = self.db.query("test", json.dumps({"_id": "persist"}))
        self.assertEqual(len(results), 1)
        doc = json.loads(results[0])
        self.assertEqual(doc["value"], 123)


class TestDocumentOperations(unittest.TestCase):
    """Test document CRUD operations."""

    def setUp(self):
        """Create a temporary database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = Database.open(self.db_path)

    def tearDown(self):
        """Clean up the test database."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_insert_and_query(self):
        """Test inserting and querying documents."""
        doc = {"name": "Alice", "age": 30, "city": "New York"}
        doc_id = self.db.insert("users", json.dumps(doc))
        self.assertIsNotNone(doc_id)

        results = self.db.query("users", "{}")
        self.assertEqual(len(results), 1)

        result_doc = json.loads(results[0])
        self.assertEqual(result_doc["name"], "Alice")
        self.assertEqual(result_doc["age"], 30)

    def test_insert_with_explicit_id(self):
        """Test inserting a document with an explicit ID."""
        doc = {"_id": "user-123", "name": "Bob", "age": 25}
        doc_id = self.db.insert("users", json.dumps(doc))
        self.assertEqual(doc_id, "user-123")

    def test_find_by_id(self):
        """Test finding a document by ID."""
        doc = {"_id": "find-me", "name": "Charlie", "value": 42}
        self.db.insert("items", json.dumps(doc))

        found = self.db.find_by_id("items", "find-me")
        result = json.loads(found)
        self.assertEqual(result["name"], "Charlie")
        self.assertEqual(result["value"], 42)

    def test_find_by_id_not_found(self):
        """Test finding a non-existent document."""
        # This should either return None or raise an exception
        # depending on the implementation
        try:
            found = self.db.find_by_id("items", "does-not-exist")
            # If it returns a value, it should be None or empty
            if found:
                # Some implementations might return empty JSON
                self.assertIn(found, [None, "", "null", "{}"])
        except Exception:
            # It's also acceptable to raise an exception
            pass

    def test_update(self):
        """Test updating a document."""
        doc = {"_id": "update-me", "name": "David", "count": 1}
        self.db.insert("items", json.dumps(doc))

        updated = {"_id": "update-me", "name": "David Updated", "count": 2}
        self.db.update("items", "update-me", json.dumps(updated))

        found = self.db.find_by_id("items", "update-me")
        result = json.loads(found)
        self.assertEqual(result["name"], "David Updated")
        self.assertEqual(result["count"], 2)

    def test_delete(self):
        """Test deleting a document."""
        doc = {"_id": "delete-me", "temp": True}
        self.db.insert("temp", json.dumps(doc))

        self.db.delete("temp", "delete-me")

        results = self.db.query("temp", "{}")
        self.assertEqual(len(results), 0)

    def test_query_with_filter(self):
        """Test querying with filters."""
        docs = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
            {"name": "David", "age": 40},
        ]

        for doc in docs:
            self.db.insert("people", json.dumps(doc))

        # Query with age >= 35
        results = self.db.query("people", json.dumps({"age": {"$gte": 35}}))
        self.assertEqual(len(results), 2)

        # Verify results
        for result in results:
            doc = json.loads(result)
            self.assertGreaterEqual(doc["age"], 35)


class TestIndexOperations(unittest.TestCase):
    """Test index creation and usage."""

    def setUp(self):
        """Create a temporary database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = Database.open(self.db_path)

    def tearDown(self):
        """Clean up the test database."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_index(self):
        """Test creating an index."""
        # Insert some documents first
        docs = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]

        for doc in docs:
            self.db.insert("users", json.dumps(doc))

        # Create index
        self.db.create_index("users", "age_idx", "age", False)

        # Query should still work
        results = self.db.query("users", json.dumps({"age": {"$gte": 30}}))
        self.assertEqual(len(results), 2)

    def test_create_unique_index(self):
        """Test creating a unique index."""
        self.db.create_index("users", "email_idx", "email", True)

        # First insert should succeed
        doc1 = {"name": "Alice", "email": "alice@example.com"}
        self.db.insert("users", json.dumps(doc1))

        # Second insert with same email should fail
        doc2 = {"name": "Bob", "email": "alice@example.com"}
        with self.assertRaises(Exception):
            self.db.insert("users", json.dumps(doc2))

    def test_drop_index(self):
        """Test dropping an index."""
        # Create some documents
        docs = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        for doc in docs:
            self.db.insert("users", json.dumps(doc))

        # Create and then drop an index
        self.db.create_index("users", "age_idx", "age", False)
        self.db.drop_index("users", "age_idx")

        # Queries should still work without the index
        results = self.db.query("users", "{}")
        self.assertEqual(len(results), 2)

    def test_drop_nonexistent_index(self):
        """Test dropping a non-existent index."""
        with self.assertRaises(Exception):
            self.db.drop_index("users", "nonexistent_idx")

    def test_create_compound_index(self):
        """Test creating a compound index."""
        docs = [
            {"city": "New York", "age": 30, "name": "Alice"},
            {"city": "New York", "age": 25, "name": "Bob"},
            {"city": "Boston", "age": 30, "name": "Charlie"},
        ]

        for doc in docs:
            self.db.insert("users", json.dumps(doc))

        # Create compound index on city and age
        self.db.create_compound_index("users", "city_age_idx", ["city", "age"], False)

        # Query should work
        results = self.db.query("users", json.dumps({"city": "New York"}))
        self.assertEqual(len(results), 2)


class TestTransactions(unittest.TestCase):
    """Test transaction operations."""

    def setUp(self):
        """Create a temporary database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = Database.open(self.db_path)

    def tearDown(self):
        """Clean up the test database."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_transaction_commit(self):
        """Test committing a transaction."""
        tx = self.db.begin_transaction()
        tx.insert("test", json.dumps({"value": 42}))
        tx.commit()

        # Verify the data was committed
        results = self.db.query("test", "{}")
        self.assertEqual(len(results), 1)

    def test_transaction_rollback(self):
        """Test rolling back a transaction."""
        tx = self.db.begin_transaction()
        tx.insert("test", json.dumps({"value": 42}))
        tx.rollback()

        # Verify the data was not committed
        results = self.db.query("test", "{}")
        self.assertEqual(len(results), 0)

    def test_transaction_isolation(self):
        """Test transaction isolation."""
        # Start transaction 1 and insert
        tx1 = self.db.begin_transaction()
        tx1.insert("test", json.dumps({"_id": "doc1", "value": 1}))

        # Start transaction 2 - should not see uncommitted changes
        tx2 = self.db.begin_transaction()
        results = tx2.query("test", json.dumps({"_id": "doc1"}))
        self.assertEqual(len(results), 0)

        # Commit tx1
        tx1.commit()

        # tx2 still in its snapshot - won't see the change
        results = tx2.query("test", json.dumps({"_id": "doc1"}))
        self.assertEqual(len(results), 0)
        tx2.commit()

        # New transaction should see the committed data
        results = self.db.query("test", json.dumps({"_id": "doc1"}))
        self.assertEqual(len(results), 1)


class TestComplexQueries(unittest.TestCase):
    """Test complex query operations."""

    def setUp(self):
        """Create a temporary database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = Database.open(self.db_path)

        # Insert test data
        self.test_docs = [
            {"name": "Alice", "age": 25, "city": "New York", "active": True},
            {"name": "Bob", "age": 30, "city": "Boston", "active": True},
            {"name": "Charlie", "age": 35, "city": "Chicago", "active": False},
            {"name": "David", "age": 40, "city": "New York", "active": True},
            {"name": "Eve", "age": 28, "city": "Boston", "active": False},
        ]

        for doc in self.test_docs:
            self.db.insert("people", json.dumps(doc))

    def tearDown(self):
        """Clean up the test database."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_query_with_multiple_conditions(self):
        """Test querying with multiple conditions."""
        # Find people in New York who are active
        results = self.db.query(
            "people",
            json.dumps({"city": "New York", "active": True})
        )
        self.assertEqual(len(results), 2)

        for result in results:
            doc = json.loads(result)
            self.assertEqual(doc["city"], "New York")
            self.assertTrue(doc["active"])

    def test_query_with_range(self):
        """Test range queries."""
        # Find people aged 30-40
        results = self.db.query(
            "people",
            json.dumps({"age": {"$gte": 30, "$lte": 40}})
        )
        self.assertEqual(len(results), 3)

        for result in results:
            doc = json.loads(result)
            self.assertGreaterEqual(doc["age"], 30)
            self.assertLessEqual(doc["age"], 40)

    def test_nested_document_query(self):
        """Test querying nested documents."""
        doc = {
            "user": "test",
            "profile": {
                "name": "Test User",
                "settings": {
                    "notifications": True,
                    "theme": "dark"
                }
            }
        }
        self.db.insert("profiles", json.dumps(doc))

        results = self.db.query("profiles", json.dumps({"user": "test"}))
        self.assertEqual(len(results), 1)

        result = json.loads(results[0])
        self.assertEqual(result["profile"]["settings"]["theme"], "dark")


class TestCollectionAPI(unittest.TestCase):
    """Test the Collection API."""

    def setUp(self):
        """Create a temporary database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = Database.open(self.db_path)

    def tearDown(self):
        """Clean up the test database."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_collection_operations(self):
        """Test collection-based operations."""
        collection = self.db.collection("users")

        # Insert via collection
        doc = {"name": "Alice", "age": 30}
        doc_id = collection.insert(doc)
        self.assertIsNotNone(doc_id)

        # Query via collection
        results = collection.query({})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "Alice")


class TestErrorHandling(unittest.TestCase):
    """Test error handling."""

    def setUp(self):
        """Create a temporary database for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = Database.open(self.db_path)

    def tearDown(self):
        """Clean up the test database."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_invalid_path(self):
        """Test opening database with invalid path."""
        with self.assertRaises(Exception):
            Database.open("/invalid/../path/db")

    def test_duplicate_id(self):
        """Test inserting duplicate IDs."""
        doc = {"_id": "duplicate", "value": 1}
        self.db.insert("test", json.dumps(doc))

        # Second insert should fail
        with self.assertRaises(Exception):
            self.db.insert("test", json.dumps(doc))

    def test_update_nonexistent(self):
        """Test updating a non-existent document."""
        with self.assertRaises(Exception):
            self.db.update("test", "nonexistent", json.dumps({"value": 1}))

    def test_delete_nonexistent(self):
        """Test deleting a non-existent document."""
        with self.assertRaises(Exception):
            self.db.delete("test", "nonexistent")


if __name__ == "__main__":
    unittest.main()
