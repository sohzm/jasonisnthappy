//go:build ignore

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	jasonisnthappy "github.com/sohzm/jasonisnthappy/bindings/go"
)

func main() {
	// Create a temporary database
	dbPath := "/tmp/watch_example.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath + ".lock")

	// Open database
	db, err := jasonisnthappy.Open(dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get collection
	coll, err := db.GetCollection("events")
	if err != nil {
		log.Fatalf("Failed to get collection: %v", err)
	}
	defer coll.Free()

	// Start watching for changes
	fmt.Println("Starting watch on 'events' collection...")
	handle, err := coll.WatchStart("", func(collection, operation, docID, docJSON string) {
		fmt.Printf("[WATCH EVENT] Collection: %s, Operation: %s, DocID: %s\n", collection, operation, docID)
		fmt.Printf("              Document: %s\n", docJSON)
	})
	if err != nil {
		log.Fatalf("Failed to start watch: %v", err)
	}
	defer handle.Stop()

	// Give the watch a moment to start
	time.Sleep(100 * time.Millisecond)

	// Insert some documents
	fmt.Println("\nInserting documents...")

	doc1 := map[string]interface{}{
		"_id":   "event1",
		"type":  "login",
		"user":  "alice",
		"timestamp": time.Now().Unix(),
	}
	if _, err := coll.Insert(doc1); err != nil {
		log.Printf("Insert error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	doc2 := map[string]interface{}{
		"_id":   "event2",
		"type":  "logout",
		"user":  "bob",
		"timestamp": time.Now().Unix(),
	}
	if _, err := coll.Insert(doc2); err != nil {
		log.Printf("Insert error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Update a document
	fmt.Println("\nUpdating document...")
	updateDoc := map[string]interface{}{
		"_id":   "event1",
		"type":  "login",
		"user":  "alice_updated",
		"timestamp": time.Now().Unix(),
	}
	if err := coll.UpdateByID("event1", updateDoc); err != nil {
		log.Printf("Update error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Delete a document
	fmt.Println("\nDeleting document...")
	if err := coll.DeleteByID("event2"); err != nil {
		log.Printf("Delete error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\nWatch example completed!")
	fmt.Println("Stopping watch...")
}
