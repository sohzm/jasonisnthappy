package jasonisnthappy

import (
	"os"
	"testing"
)

func TestDropIndexImplementation(t *testing.T) {
	// Create temp database
	dbPath := "/tmp/test_drop_index.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath + ".lock")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create an index
	err = db.CreateIndex("users", "age_idx", "age", false)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	t.Log("✅ Created index: age_idx")

	// List indexes to verify it was created
	indexes, err := db.ListIndexes("users")
	if err != nil {
		t.Fatalf("Failed to list indexes: %v", err)
	}

	found := false
	for _, idx := range indexes {
		if idx.Name == "age_idx" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Index age_idx not found after creation")
	}
	t.Log("✅ Verified index exists")

	// Drop the index (THIS IS THE NEW FEATURE!)
	err = db.DropIndex("users", "age_idx")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}
	t.Log("✅ Dropped index: age_idx")

	// Verify it was dropped
	indexes, err = db.ListIndexes("users")
	if err != nil {
		t.Fatalf("Failed to list indexes after drop: %v", err)
	}

	for _, idx := range indexes {
		if idx.Name == "age_idx" {
			t.Fatal("Index age_idx still exists after drop!")
		}
	}
	t.Log("✅ Verified index was dropped successfully")

	// Try to drop again - should fail
	err = db.DropIndex("users", "age_idx")
	if err == nil {
		t.Fatal("Expected error when dropping non-existent index")
	}
	t.Logf("✅ Correctly returned error for non-existent index: %v", err)
}

func TestBasicCRUD(t *testing.T) {
	// Create temp database
	dbPath := "/tmp/test_crud.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath + ".lock")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get collection
	coll, err := db.GetCollection("users")
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}
	defer coll.Free()

	// Insert
	doc := map[string]interface{}{
		"_id":  "user1",
		"name": "Alice",
		"age":  30,
	}
	id, err := coll.Insert(doc)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	t.Logf("✅ Inserted document with ID: %s", id)

	// Find by ID
	var result map[string]interface{}
	found, err := coll.FindByID("user1", &result)
	if err != nil {
		t.Fatalf("Failed to find by ID: %v", err)
	}
	if !found {
		t.Fatal("Document not found")
	}
	if result["name"] != "Alice" {
		t.Fatalf("Expected name 'Alice', got '%v'", result["name"])
	}
	t.Log("✅ Found document by ID")

	// Update
	doc["age"] = 31
	err = coll.UpdateByID("user1", doc)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	t.Log("✅ Updated document")

	// Verify update
	found, err = coll.FindByID("user1", &result)
	if err != nil || !found {
		t.Fatal("Failed to find updated document")
	}
	if result["age"].(float64) != 31 {
		t.Fatalf("Expected age 31, got %v", result["age"])
	}
	t.Log("✅ Verified update")

	// Delete
	err = coll.DeleteByID("user1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	t.Log("✅ Deleted document")

	// Verify deletion
	found, err = coll.FindByID("user1", &result)
	if err != nil {
		t.Fatalf("Error during find: %v", err)
	}
	if found {
		t.Fatal("Document still exists after deletion")
	}
	t.Log("✅ Verified deletion")
}
