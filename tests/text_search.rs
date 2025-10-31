//! Consolidated Text Search Test Suite
//!
//! This file contains all text search and indexing tests for jasonisnthappy.
//! Tests are organized into sections covering different aspects of text search functionality.

use jasonisnthappy::Database;
use serde_json::json;
use tempfile::tempdir;

// ============================================================================
// Index Creation Tests
// ============================================================================

#[test]
fn test_create_text_index() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");
    posts.insert(json!({
        "title": "Introduction to Rust",
        "body": "Rust is a systems programming language."
    })).unwrap();

    // Create text index
    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Verify index was created (will test search separately)
}

#[test]
fn test_text_search_single_field() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    let doc1 = posts.insert(json!({
        "title": "Rust Tutorial",
        "body": "Learn Python"
    })).unwrap();

    let _doc2 = posts.insert(json!({
        "title": "Python Guide",
        "body": "Learn Rust"
    })).unwrap();

    // Index only the title field
    db.create_text_index("posts", "title_idx", &["title"]).unwrap();

    // Search for "rust" - should only find doc1 (in title)
    let results = posts.search("rust").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, doc1);
}

#[test]
fn test_text_search_existing_documents() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    // Insert documents BEFORE creating index
    let doc1 = posts.insert(json!({
        "title": "Rust Programming",
        "body": "Systems language"
    })).unwrap();

    let _doc2 = posts.insert(json!({
        "title": "Go Programming",
        "body": "Concurrent language"
    })).unwrap();

    // Create index - should index existing documents
    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Search should find existing documents
    let results = posts.search("rust").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, doc1);
}

// ============================================================================
// Basic Search Tests
// ============================================================================

#[test]
fn test_text_search_basic() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    // Insert test documents
    let doc1 = posts.insert(json!({
        "title": "Introduction to Rust",
        "body": "Rust is a systems programming language that focuses on safety and performance."
    })).unwrap();

    let doc2 = posts.insert(json!({
        "title": "Getting started with databases",
        "body": "Learn how to build a database from scratch using Rust."
    })).unwrap();

    let doc3 = posts.insert(json!({
        "title": "Web development with Python",
        "body": "Python is great for web development and data science."
    })).unwrap();

    // Create text index
    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Search for "rust"
    let results = posts.search("rust").unwrap();
    assert_eq!(results.len(), 2);

    // Verify both doc1 and doc2 are in results (both mention "rust")
    let result_ids: Vec<String> = results.iter().map(|r| r.doc_id.clone()).collect();
    assert!(result_ids.contains(&doc1));
    assert!(result_ids.contains(&doc2));
    assert!(!result_ids.contains(&doc3));
}

#[test]
fn test_text_search_no_results() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    posts.insert(json!({
        "title": "Rust Programming",
        "body": "Learn Rust"
    })).unwrap();

    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Search for term that doesn't exist
    let results = posts.search("python").unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_text_search_empty_query() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    posts.insert(json!({
        "title": "Test",
        "body": "Test content"
    })).unwrap();

    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Empty query should return no results
    let results = posts.search("").unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_text_search_without_index() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    posts.insert(json!({
        "title": "Test",
        "body": "Content"
    })).unwrap();

    // Try to search without creating an index
    let result = posts.search("test");
    assert!(result.is_err());
}

// ============================================================================
// Ranking and Scoring Tests
// ============================================================================

#[test]
fn test_text_search_ranking() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    // doc1 mentions "database" multiple times
    let doc1 = posts.insert(json!({
        "title": "Database Design",
        "body": "Database design is important. A good database architecture ensures your database performs well."
    })).unwrap();

    // doc2 mentions "database" once
    let _doc2 = posts.insert(json!({
        "title": "Software Engineering",
        "body": "Software engineering involves many aspects including database design."
    })).unwrap();

    // Create text index and search
    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();
    let results = posts.search("database").unwrap();

    // doc1 should rank higher (mentioned more times)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].doc_id, doc1);
    assert!(results[0].score > results[1].score);
}

#[test]
fn test_text_search_multiple_terms() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    let doc1 = posts.insert(json!({
        "title": "Rust Programming",
        "body": "Building a database in Rust"
    })).unwrap();

    let doc2 = posts.insert(json!({
        "title": "Database Systems",
        "body": "Understanding database internals"
    })).unwrap();

    let _doc3 = posts.insert(json!({
        "title": "Python Tutorial",
        "body": "Learn Python programming"
    })).unwrap();

    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Search for multiple terms
    let results = posts.search("rust database").unwrap();

    // doc1 contains both terms, should be first
    assert_eq!(results[0].doc_id, doc1);

    // doc2 contains only "database", should be second
    assert!(results.len() >= 2);
    assert_eq!(results[1].doc_id, doc2);
}

// ============================================================================
// Case Sensitivity and Unicode Tests
// ============================================================================

#[test]
fn test_text_search_case_insensitive() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    let doc_id = posts.insert(json!({
        "title": "RUST Programming",
        "body": "Learn RUST language"
    })).unwrap();

    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Search should be case-insensitive
    let results1 = posts.search("rust").unwrap();
    let results2 = posts.search("RUST").unwrap();
    let results3 = posts.search("Rust").unwrap();

    assert_eq!(results1.len(), 1);
    assert_eq!(results2.len(), 1);
    assert_eq!(results3.len(), 1);
    assert_eq!(results1[0].doc_id, doc_id);
}

#[test]
fn test_text_search_special_characters() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    let doc_id = posts.insert(json!({
        "title": "Hello, World!",
        "body": "Testing special chars: @#$% & more!"
    })).unwrap();

    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Search for words without punctuation
    let results = posts.search("hello world").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, doc_id);
}

#[test]
fn test_text_search_unicode() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    let doc_id = posts.insert(json!({
        "title": "Rust is 🔥 amazing!",
        "body": "Unicode support is important"
    })).unwrap();

    db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();

    // Search should handle unicode properly
    let results = posts.search("amazing").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, doc_id);
}

// ============================================================================
// Persistence and Database Lifecycle Tests
// ============================================================================

#[test]
fn test_text_index_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let doc_id = {
        let db = Database::open(db_path.to_str().unwrap()).unwrap();
        let posts = db.collection("posts");

        let id = posts.insert(json!({
            "title": "Persistent Search",
            "body": "This should work after reopening"
        })).unwrap();

        db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();
        id
    };

    // Reopen database and search
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let posts = db.collection("posts");

    let results = posts.search("persistent").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, doc_id);
}

#[test]
fn test_simple_index_and_search() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // Create and populate database
    {
        let db = Database::open(db_path.to_str().unwrap()).unwrap();
        let posts = db.collection("posts");

        posts.insert(json!({
            "title": "hello",
            "body": "world"
        })).unwrap();

        // Create index
        db.create_text_index("posts", "idx", &["title"]).unwrap();

        // Force flush
        db.checkpoint().unwrap();
    }

    // Reopen and search
    {
        let db = Database::open(db_path.to_str().unwrap()).unwrap();
        let posts = db.collection("posts");

        let results = posts.search("hello").unwrap();
        println!("Results: {:?}", results);
        assert_eq!(results.len(), 1, "Expected 1 result, got {}", results.len());
    }
}

// ============================================================================
// Debug and Diagnostic Tests
// ============================================================================

#[test]
fn test_debug_text_index() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let posts = db.collection("posts");

    // Insert a simple document
    let doc_id = posts.insert(json!({
        "title": "test",
        "body": "content"
    })).unwrap();

    println!("Inserted document: {}", doc_id);

    // Create text index
    let result = db.create_text_index("posts", "search_idx", &["title", "body"]);
    println!("Create index result: {:?}", result);
    assert!(result.is_ok());

    // Try to search
    let search_result = posts.search("test");
    println!("Search result: {:?}", search_result);

    match search_result {
        Ok(results) => {
            println!("Found {} results", results.len());
            for r in &results {
                println!("  - doc_id: {}, score: {}", r.doc_id, r.score);
            }
            assert!(results.len() > 0, "Expected at least one result");
        }
        Err(e) => {
            panic!("Search failed: {:?}", e);
        }
    }
}
