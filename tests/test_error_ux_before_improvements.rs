/// This test demonstrates the CURRENT poor error UX before improvements
///
/// Problems demonstrated:
/// 1. Error::Other() is used for business logic errors (hard to pattern match)
/// 2. Missing context about which collection/document caused the error
/// 3. Generic error messages that aren't actionable
/// 4. Difficult to programmatically distinguish between different error types
///
/// After improvements, these errors will have:
/// - Specific enum variants (CollectionAlreadyExists, DocumentAlreadyExists, etc.)
/// - Rich context (collection name, document ID, operation)
/// - Actionable messages
/// - Easy pattern matching

use jasonisnthappy::{Database, Error};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn test_current_poor_error_ux() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    println!("\n========================================");
    println!("ERROR UX IMPROVEMENTS DEMO");
    println!("========================================\n");

    // ============================================================================
    // ✓ FIXED Problem 1: Collection Already Exists - Now uses specific variant
    // ============================================================================
    println!("--- ✓ FIXED Problem 1: Collection Already Exists Error ---");

    let mut tx = db.begin().unwrap();
    tx.create_collection("users").unwrap();

    // Try to create the same collection again
    let err = tx.create_collection("users").unwrap_err();

    println!("Error: {:?}", err);
    println!("Error Display: {}", err);

    // NOW: We can pattern match on a specific variant!
    match err {
        Error::CollectionAlreadyExists { name } => {
            println!("✓ GOOD: Specific error variant (not Error::Other)");
            println!("✓ GOOD: Can extract collection name programmatically: '{}'", name);
            println!("✓ GOOD: Type-safe pattern matching");
            assert_eq!(name, "users");
        }
        _ => panic!("Expected Error::CollectionAlreadyExists"),
    }

    tx.rollback().unwrap();

    // ============================================================================
    // ✓ FIXED Problem 2: Collection Does Not Exist - Now uses specific variant
    // ============================================================================
    println!("\n--- ✓ FIXED Problem 2: Collection Does Not Exist Error ---");

    let mut tx = db.begin().unwrap();

    // Try to rename a non-existent collection
    let err = tx.rename_collection("nonexistent", "newname").unwrap_err();

    println!("Error: {:?}", err);
    println!("Error Display: {}", err);

    // NOW: Specific variant that's different from CollectionAlreadyExists
    match err {
        Error::CollectionDoesNotExist { name } => {
            println!("✓ GOOD: Distinct error variant (not Error::Other)");
            println!("✓ GOOD: Clear semantic difference from 'already exists'");
            println!("✓ GOOD: Can extract collection name: '{}'", name);
            assert_eq!(name, "nonexistent");
        }
        _ => panic!("Expected Error::CollectionDoesNotExist"),
    }

    tx.rollback().unwrap();

    // ============================================================================
    // ✓ FIXED Problem 3: Document Already Exists - Now uses specific variant
    // ============================================================================
    println!("\n--- ✓ FIXED Problem 3: Document Already Exists Error ---");

    let mut tx = db.begin().unwrap();
    tx.create_collection("products").unwrap();
    let mut coll = tx.collection("products").unwrap();

    // Insert a document with explicit ID
    let doc = json!({
        "_id": "prod_123",
        "name": "Widget",
        "price": 9.99
    });
    coll.insert(doc.clone()).unwrap();

    // Try to insert the same ID again
    let err = coll.insert(doc).unwrap_err();

    println!("Error: {:?}", err);
    println!("Error Display: {}", err);

    match err {
        Error::DocumentAlreadyExists { collection, id } => {
            println!("✓ GOOD: Specific DocumentAlreadyExists variant");
            println!("✓ GOOD: Can extract collection: '{}'", collection);
            println!("✓ GOOD: Can extract document ID: '{}'", id);
            println!("✓ GOOD: Distinct from CollectionAlreadyExists (type-safe)");
            assert_eq!(collection, "products");
            assert_eq!(id, "prod_123");
        }
        _ => panic!("Expected Error::DocumentAlreadyExists"),
    }

    tx.rollback().unwrap();

    // ============================================================================
    // ✓ FIXED Problem 4: Invalid Document Format - Now uses specific variant with context
    // ============================================================================
    println!("\n--- ✓ FIXED Problem 4: Invalid Document Format Error ---");

    let mut tx = db.begin().unwrap();
    tx.create_collection("orders").unwrap();
    let mut coll = tx.collection("orders").unwrap();

    // Try to insert a non-object document (should be rejected)
    let invalid_doc = json!("just a string");
    let err = coll.insert(invalid_doc).unwrap_err();

    println!("Error: {:?}", err);
    println!("Error Display: {}", err);

    match err {
        Error::InvalidDocumentFormat { reason, collection } => {
            println!("✓ GOOD: Specific InvalidDocumentFormat variant");
            println!("✓ GOOD: Reason provided: '{}'", reason);
            println!("✓ GOOD: Collection context: {:?}", collection);
            println!("✓ GOOD: Type-safe, actionable error");
            assert_eq!(reason, "document must be an object");
            assert_eq!(collection, Some("orders".to_string()));
        }
        _ => panic!("Expected Error::InvalidDocumentFormat"),
    }

    tx.rollback().unwrap();

    // ============================================================================
    // PROBLEM 5: Cannot Programmatically Handle Errors
    // ============================================================================
    println!("\n--- Problem 5: Error Handling Requires String Matching ---");

    println!("✗ BAD: To distinguish between different Error::Other cases:");
    println!("       - Must use string matching (fragile, not type-safe)");
    println!("       - Cannot use exhaustive pattern matching");
    println!("       - Error messages might change, breaking client code");
    println!("       - Cannot extract structured data (names, IDs) from errors");

    // Example of bad error handling code currently required:
    fn handle_error_badly(err: Error) -> String {
        match err {
            Error::Other(msg) => {
                // Fragile string matching - error prone!
                if msg.contains("already exists") {
                    "Duplicate item".to_string()
                } else if msg.contains("does not exist") {
                    "Not found".to_string()
                } else if msg.contains("must be an object") {
                    "Invalid format".to_string()
                } else {
                    "Unknown error".to_string()
                }
            }
            _ => "Other error type".to_string(),
        }
    }

    let test_err = Error::Other("collection 'test' already exists".to_string());
    println!("       Example: handle_error_badly() -> {}", handle_error_badly(test_err));

    // ============================================================================
    // PROBLEM 6: Serialization Errors - No Context
    // ============================================================================
    println!("\n--- Problem 6: Serialization Errors Lack Context ---");

    let mut tx = db.begin().unwrap();
    tx.create_collection("items").unwrap();
    let _coll = tx.collection("items");

    // This will cause a serialization error if we had circular references
    // or invalid JSON (hard to demonstrate without unsafe code)
    // But the error would look like:
    println!("Example serialization error:");
    println!("  Error::Other(\"Failed to serialize document: <json error>\")");
    println!("✗ BAD: Doesn't tell us:");
    println!("       - Which collection was being written to");
    println!("       - Which document ID");
    println!("       - What field caused the problem");
    println!("       - Was it serialization or deserialization?");

    tx.rollback().unwrap();

    println!("\n========================================");
    println!("SUMMARY OF PROBLEMS");
    println!("========================================");
    println!("1. ✗ 15+ uses of Error::Other for business logic");
    println!("2. ✗ String matching required (fragile, not type-safe)");
    println!("3. ✗ Missing context (collection, document, operation)");
    println!("4. ✗ Cannot extract structured data from errors");
    println!("5. ✗ Generic messages not actionable");
    println!("6. ✗ No distinction between similar error types");
    println!("\nAFTER IMPROVEMENTS:");
    println!("1. ✓ Specific error variants for each case");
    println!("2. ✓ Pattern matching on enum variants");
    println!("3. ✓ Rich context in error fields");
    println!("4. ✓ Structured data extraction");
    println!("5. ✓ Actionable error messages");
    println!("6. ✓ Clear type-safe error handling");
    println!("========================================\n");
}

#[test]
fn test_what_good_errors_would_look_like() {
    println!("\n========================================");
    println!("WHAT GOOD ERRORS WOULD LOOK LIKE");
    println!("========================================\n");

    println!("Instead of:");
    println!("  Error::Other(\"collection 'users' already exists\")");
    println!("\nWe want:");
    println!("  Error::CollectionAlreadyExists {{ name: \"users\" }}");
    println!("  - Pattern matchable");
    println!("  - Can extract name programmatically");
    println!("  - Type-safe\n");

    println!("Instead of:");
    println!("  Error::Other(\"document with ID user_123 already exists\")");
    println!("\nWe want:");
    println!("  Error::DocumentAlreadyExists {{ collection: \"users\", id: \"user_123\" }}");
    println!("  - Know which collection");
    println!("  - Know which document");
    println!("  - Can handle in client code\n");

    println!("Instead of:");
    println!("  Error::Other(\"Failed to serialize document: invalid value\")");
    println!("\nWe want:");
    println!("  Error::SerializationError {{");
    println!("    context: \"collection 'users', document 'user_123'\",");
    println!("    error: \"invalid value at field 'email'\"");
    println!("  }}");
    println!("  - Know what was being serialized");
    println!("  - Know where the error occurred");
    println!("  - Actionable for debugging\n");

    println!("========================================\n");
}
