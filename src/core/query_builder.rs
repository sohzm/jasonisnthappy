use crate::core::collection::Collection;
use crate::core::errors::*;
use crate::core::query::parser::parse_query;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

pub struct QueryBuilder<'a> {
    collection: &'a Collection,
    query: Option<String>,
    sort_fields: Vec<(String, SortOrder)>,
    limit_count: Option<usize>,
    skip_count: usize,
    projection: Option<Projection>,
}

#[derive(Debug, Clone)]
enum Projection {
    Include(Vec<String>),
    Exclude(Vec<String>),
}

impl<'a> QueryBuilder<'a> {
    pub(crate) fn new(collection: &'a Collection) -> Self {
        Self {
            collection,
            query: None,
            sort_fields: Vec::new(),
            limit_count: None,
            skip_count: 0,
            projection: None,
        }
    }

    /// Filter documents using a query string
    pub fn filter(mut self, query: &str) -> Self {
        self.query = Some(query.to_string());
        self
    }

    /// Sort results by a field in the specified order
    pub fn sort_by(mut self, field: &str, order: SortOrder) -> Self {
        self.sort_fields.push((field.to_string(), order));
        self
    }

    /// Limit the number of results returned
    pub fn limit(mut self, n: usize) -> Self {
        self.limit_count = Some(n);
        self
    }

    /// Skip the first N results
    pub fn skip(mut self, n: usize) -> Self {
        self.skip_count = n;
        self
    }

    /// Project only the specified fields (inclusion projection)
    /// _id is always included unless explicitly excluded
    pub fn project(mut self, fields: &[&str]) -> Self {
        self.projection = Some(Projection::Include(
            fields.iter().map(|s| s.to_string()).collect()
        ));
        self
    }

    /// Exclude the specified fields from results (exclusion projection)
    pub fn exclude(mut self, fields: &[&str]) -> Self {
        self.projection = Some(Projection::Exclude(
            fields.iter().map(|s| s.to_string()).collect()
        ));
        self
    }

    /// Execute the query and return results
    pub fn execute(self) -> Result<Vec<Value>> {
        // Step 1: Get all documents (filtered if query specified)
        let mut results = if let Some(q) = &self.query {
            let ast = parse_query(q)
                .map_err(|e| Error::Other(format!("failed to parse query: {}", e)))?;

            let all_docs = match self.collection.find_all() {
                Ok(docs) => docs,
                Err(Error::Other(msg)) if msg.contains("not found") => Vec::new(),
                Err(e) => return Err(e),
            };
            all_docs
                .into_iter()
                .filter(|doc| {
                    if let Some(doc_map) = doc.as_object() {
                        ast.eval(doc_map)
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            match self.collection.find_all() {
                Ok(docs) => docs,
                Err(Error::Other(msg)) if msg.contains("not found") => Vec::new(),
                Err(e) => return Err(e),
            }
        };

        // Step 2: Apply sorting
        if !self.sort_fields.is_empty() {
            results.sort_by(|a, b| {
                for (field, order) in &self.sort_fields {
                    let val_a = get_nested_field(a, field);
                    let val_b = get_nested_field(b, field);

                    let cmp = compare_values(&val_a, &val_b);
                    let cmp = match order {
                        SortOrder::Asc => cmp,
                        SortOrder::Desc => cmp.reverse(),
                    };

                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Step 3: Apply skip
        let results: Vec<Value> = results.into_iter().skip(self.skip_count).collect();

        // Step 4: Apply limit
        let results = if let Some(limit) = self.limit_count {
            results.into_iter().take(limit).collect()
        } else {
            results
        };

        // Step 5: Apply projection
        let results = if let Some(projection) = &self.projection {
            results
                .into_iter()
                .map(|doc| apply_projection(doc, projection))
                .collect()
        } else {
            results
        };

        Ok(results)
    }

    /// Execute and return the first result
    pub fn first(self) -> Result<Option<Value>> {
        let mut builder = self;
        builder.limit_count = Some(1);
        let results = builder.execute()?;
        Ok(results.into_iter().next())
    }

    /// Count results without fetching them all
    pub fn count(self) -> Result<usize> {
        // For count, we don't need to sort or apply limit
        let results = if let Some(q) = &self.query {
            let ast = parse_query(q)
                .map_err(|e| Error::Other(format!("failed to parse query: {}", e)))?;

            let all_docs = match self.collection.find_all() {
                Ok(docs) => docs,
                Err(Error::Other(msg)) if msg.contains("not found") => Vec::new(),
                Err(e) => return Err(e),
            };
            all_docs
                .into_iter()
                .filter(|doc| {
                    if let Some(doc_map) = doc.as_object() {
                        ast.eval(doc_map)
                    } else {
                        false
                    }
                })
                .count()
        } else {
            match self.collection.count() {
                Ok(count) => count,
                Err(Error::Other(msg)) if msg.contains("not found") => 0,
                Err(e) => return Err(e),
            }
        };

        // Apply skip to count
        if self.skip_count >= results {
            Ok(0)
        } else {
            let after_skip = results - self.skip_count;
            // Apply limit to count if specified
            Ok(self.limit_count.map_or(after_skip, |limit| limit.min(after_skip)))
        }
    }
}

/// Extract a potentially nested field from a Value
fn get_nested_field(value: &Value, field: &str) -> Value {
    let parts: Vec<&str> = field.split('.').collect();
    let mut current = value.clone();

    for part in parts {
        if let Some(obj) = current.as_object() {
            current = obj.get(part).cloned().unwrap_or(Value::Null);
        } else {
            return Value::Null;
        }
    }

    current
}

/// Compare two JSON values for sorting
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        // Null values sort first
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,

        // Numbers
        (Value::Number(na), Value::Number(nb)) => {
            let fa = na.as_f64().unwrap_or(0.0);
            let fb = nb.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }

        // Strings
        (Value::String(sa), Value::String(sb)) => sa.cmp(sb),

        // Booleans
        (Value::Bool(ba), Value::Bool(bb)) => ba.cmp(bb),

        // Arrays - compare lexicographically
        (Value::Array(aa), Value::Array(ab)) => {
            for (item_a, item_b) in aa.iter().zip(ab.iter()) {
                let cmp = compare_values(item_a, item_b);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            aa.len().cmp(&ab.len())
        }

        // Objects - not really comparable, compare as JSON strings
        (Value::Object(_), Value::Object(_)) => {
            let sa = serde_json::to_string(a).unwrap_or_default();
            let sb = serde_json::to_string(b).unwrap_or_default();
            sa.cmp(&sb)
        }

        // Different types - order by type precedence
        _ => {
            let type_order = |v: &Value| match v {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Number(_) => 2,
                Value::String(_) => 3,
                Value::Array(_) => 4,
                Value::Object(_) => 5,
            };
            type_order(a).cmp(&type_order(b))
        }
    }
}

/// Apply projection to a document
fn apply_projection(doc: Value, projection: &Projection) -> Value {
    if let Value::Object(obj) = doc {
        match projection {
            Projection::Include(fields) => {
                let mut result = serde_json::Map::new();

                // Always include _id unless explicitly excluded
                if let Some(id) = obj.get("_id") {
                    result.insert("_id".to_string(), id.clone());
                }

                // Include specified fields
                for field in fields {
                    if field == "_id" {
                        // Already handled above
                        continue;
                    }

                    if field.contains('.') {
                        // Handle nested fields
                        if let Some(value) = get_nested_field_from_map(&obj, field) {
                            set_nested_field(&mut result, field, value);
                        }
                    } else {
                        // Simple field
                        if let Some(value) = obj.get(field) {
                            result.insert(field.clone(), value.clone());
                        }
                    }
                }

                Value::Object(result)
            }
            Projection::Exclude(fields) => {
                let mut result = obj.clone();

                // Remove excluded fields
                for field in fields {
                    if field.contains('.') {
                        // Handle nested fields
                        remove_nested_field(&mut result, field);
                    } else {
                        // Simple field
                        result.remove(field);
                    }
                }

                Value::Object(result)
            }
        }
    } else {
        doc
    }
}

/// Get a nested field value from a map
fn get_nested_field_from_map(obj: &serde_json::Map<String, Value>, path: &str) -> Option<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = Value::Object(obj.clone());

    for part in parts {
        if let Some(obj) = current.as_object() {
            current = obj.get(part)?.clone();
        } else {
            return None;
        }
    }

    Some(current)
}

/// Set a nested field in a map
fn set_nested_field(obj: &mut serde_json::Map<String, Value>, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    set_nested_field_recursive(obj, &parts, value);
}

fn set_nested_field_recursive(obj: &mut serde_json::Map<String, Value>, parts: &[&str], value: Value) {
    if parts.is_empty() {
        return;
    }

    if parts.len() == 1 {
        obj.insert(parts[0].to_string(), value);
        return;
    }

    // Ensure the intermediate object exists
    if !obj.contains_key(parts[0]) {
        obj.insert(parts[0].to_string(), Value::Object(serde_json::Map::new()));
    }

    // Recurse into the nested object
    if let Some(Value::Object(nested)) = obj.get_mut(parts[0]) {
        set_nested_field_recursive(nested, &parts[1..], value);
    }
}

/// Remove a nested field from a map
fn remove_nested_field(obj: &mut serde_json::Map<String, Value>, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    remove_nested_field_recursive(obj, &parts);
}

fn remove_nested_field_recursive(obj: &mut serde_json::Map<String, Value>, parts: &[&str]) {
    if parts.is_empty() {
        return;
    }

    if parts.len() == 1 {
        obj.remove(parts[0]);
        return;
    }

    // Recurse into the nested object
    if let Some(Value::Object(nested)) = obj.get_mut(parts[0]) {
        remove_nested_field_recursive(nested, &parts[1..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::database::Database;
    use serde_json::json;
    use std::sync::Arc;
    use std::fs;

    fn setup_test_db(path: &str) -> (Arc<Database>, Collection) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = db.collection("users");
        (db, coll)
    }

    fn cleanup_test_db(path: &str, db: Arc<Database>) {
        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_sort_by_number_asc() {
        let path = "/tmp/test_sort_asc.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let results = coll.query()
            .sort_by("age", SortOrder::Asc)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["age"], 25);
        assert_eq!(results[1]["age"], 30);
        assert_eq!(results[2]["age"], 35);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_sort_by_number_desc() {
        let path = "/tmp/test_sort_desc.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let results = coll.query()
            .sort_by("age", SortOrder::Desc)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["age"], 35);
        assert_eq!(results[1]["age"], 30);
        assert_eq!(results[2]["age"], 25);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_sort_by_string() {
        let path = "/tmp/test_sort_string.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Charlie"})).unwrap();
        coll.insert(json!({"name": "Alice"})).unwrap();
        coll.insert(json!({"name": "Bob"})).unwrap();

        let results = coll.query()
            .sort_by("name", SortOrder::Asc)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["name"], "Alice");
        assert_eq!(results[1]["name"], "Bob");
        assert_eq!(results[2]["name"], "Charlie");

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_multi_field_sort() {
        let path = "/tmp/test_multi_sort.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"city": "NYC", "age": 30})).unwrap();
        coll.insert(json!({"city": "NYC", "age": 25})).unwrap();
        coll.insert(json!({"city": "LA", "age": 35})).unwrap();
        coll.insert(json!({"city": "LA", "age": 20})).unwrap();

        let results = coll.query()
            .sort_by("city", SortOrder::Asc)
            .sort_by("age", SortOrder::Desc)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 4);
        // LA sorted first, then by age desc within LA
        assert_eq!(results[0]["city"], "LA");
        assert_eq!(results[0]["age"], 35);
        assert_eq!(results[1]["city"], "LA");
        assert_eq!(results[1]["age"], 20);
        // NYC sorted second, then by age desc within NYC
        assert_eq!(results[2]["city"], "NYC");
        assert_eq!(results[2]["age"], 30);
        assert_eq!(results[3]["city"], "NYC");
        assert_eq!(results[3]["age"], 25);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_limit() {
        let path = "/tmp/test_limit.db";
        let (db, coll) = setup_test_db(path);

        for i in 1..=10 {
            coll.insert(json!({"number": i})).unwrap();
        }

        let results = coll.query()
            .sort_by("number", SortOrder::Asc)
            .limit(5)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0]["number"], 1);
        assert_eq!(results[4]["number"], 5);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_skip() {
        let path = "/tmp/test_skip.db";
        let (db, coll) = setup_test_db(path);

        for i in 1..=10 {
            coll.insert(json!({"number": i})).unwrap();
        }

        let results = coll.query()
            .sort_by("number", SortOrder::Asc)
            .skip(5)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0]["number"], 6);
        assert_eq!(results[4]["number"], 10);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_skip_and_limit() {
        let path = "/tmp/test_skip_limit.db";
        let (db, coll) = setup_test_db(path);

        for i in 1..=20 {
            coll.insert(json!({"number": i})).unwrap();
        }

        // Page 2: skip 10, take 10
        let results = coll.query()
            .sort_by("number", SortOrder::Asc)
            .skip(10)
            .limit(10)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 10);
        assert_eq!(results[0]["number"], 11);
        assert_eq!(results[9]["number"], 20);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_filter_sort_limit() {
        let path = "/tmp/test_filter_sort_limit.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30, "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25, "city": "LA"})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35, "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "David", "age": 40, "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "Eve", "age": 28, "city": "LA"})).unwrap();

        let results = coll.query()
            .filter("city is \"NYC\"")
            .sort_by("age", SortOrder::Desc)
            .limit(2)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["name"], "David");
        assert_eq!(results[0]["age"], 40);
        assert_eq!(results[1]["name"], "Charlie");
        assert_eq!(results[1]["age"], 35);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_nested_field_sort() {
        let path = "/tmp/test_nested_sort.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "address": {"city": "NYC"}})).unwrap();
        coll.insert(json!({"name": "Bob", "address": {"city": "LA"}})).unwrap();
        coll.insert(json!({"name": "Charlie", "address": {"city": "Boston"}})).unwrap();

        let results = coll.query()
            .sort_by("address.city", SortOrder::Asc)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["address"]["city"], "Boston");
        assert_eq!(results[1]["address"]["city"], "LA");
        assert_eq!(results[2]["address"]["city"], "NYC");

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_first() {
        let path = "/tmp/test_first.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let result = coll.query()
            .sort_by("age", SortOrder::Asc)
            .first()
            .unwrap();

        assert!(result.is_some());
        let doc = result.unwrap();
        assert_eq!(doc["name"], "Bob");
        assert_eq!(doc["age"], 25);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_first_with_filter() {
        let path = "/tmp/test_first_filter.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let result = coll.query()
            .filter("age > 28")
            .sort_by("age", SortOrder::Asc)
            .first()
            .unwrap();

        assert!(result.is_some());
        let doc = result.unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_first_empty() {
        let path = "/tmp/test_first_empty.db";
        let (db, coll) = setup_test_db(path);

        let result = coll.query()
            .first()
            .unwrap();

        assert!(result.is_none());

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_count() {
        let path = "/tmp/test_count.db";
        let (db, coll) = setup_test_db(path);

        for i in 1..=20 {
            coll.insert(json!({"number": i})).unwrap();
        }

        let count = coll.query().count().unwrap();
        assert_eq!(count, 20);

        let count = coll.query()
            .filter("number > 10")
            .count()
            .unwrap();
        assert_eq!(count, 10);

        let count = coll.query()
            .filter("number > 10")
            .skip(5)
            .count()
            .unwrap();
        assert_eq!(count, 5);

        let count = coll.query()
            .skip(5)
            .limit(10)
            .count()
            .unwrap();
        assert_eq!(count, 10);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_sort_with_null_values() {
        let path = "/tmp/test_sort_null.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob"})).unwrap();  // No age field
        coll.insert(json!({"name": "Charlie", "age": 25})).unwrap();

        let results = coll.query()
            .sort_by("age", SortOrder::Asc)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 3);
        // Null should sort first
        assert_eq!(results[0]["name"], "Bob");
        assert_eq!(results[1]["age"], 25);
        assert_eq!(results[2]["age"], 30);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_pagination() {
        let path = "/tmp/test_pagination.db";
        let (db, coll) = setup_test_db(path);

        // Insert 25 documents
        for i in 1..=25 {
            coll.insert(json!({"id": i, "value": i * 10})).unwrap();
        }

        let page_size = 10;

        // Page 1
        let page1 = coll.query()
            .sort_by("id", SortOrder::Asc)
            .limit(page_size)
            .skip(0)
            .execute()
            .unwrap();
        assert_eq!(page1.len(), 10);
        assert_eq!(page1[0]["id"], 1);
        assert_eq!(page1[9]["id"], 10);

        // Page 2
        let page2 = coll.query()
            .sort_by("id", SortOrder::Asc)
            .limit(page_size)
            .skip(10)
            .execute()
            .unwrap();
        assert_eq!(page2.len(), 10);
        assert_eq!(page2[0]["id"], 11);
        assert_eq!(page2[9]["id"], 20);

        // Page 3 (partial)
        let page3 = coll.query()
            .sort_by("id", SortOrder::Asc)
            .limit(page_size)
            .skip(20)
            .execute()
            .unwrap();
        assert_eq!(page3.len(), 5);
        assert_eq!(page3[0]["id"], 21);
        assert_eq!(page3[4]["id"], 25);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_include() {
        let path = "/tmp/test_projection_include.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "city": "NYC"
        })).unwrap();

        let results = coll.query()
            .project(&["name", "email"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        let doc = &results[0];

        // Should have _id, name, and email
        assert!(doc.get("_id").is_some());
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["email"], "alice@example.com");

        // Should NOT have age or city
        assert!(doc.get("age").is_none());
        assert!(doc.get("city").is_none());

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_exclude() {
        let path = "/tmp/test_projection_exclude.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "city": "NYC"
        })).unwrap();

        let results = coll.query()
            .exclude(&["email", "city"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        let doc = &results[0];

        // Should have _id, name, and age
        assert!(doc.get("_id").is_some());
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);

        // Should NOT have email or city
        assert!(doc.get("email").is_none());
        assert!(doc.get("city").is_none());

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_nested_fields() {
        let path = "/tmp/test_projection_nested.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({
            "name": "Alice",
            "profile": {
                "age": 30,
                "email": "alice@example.com"
            },
            "address": {
                "city": "NYC",
                "zip": "10001"
            }
        })).unwrap();

        let results = coll.query()
            .project(&["name", "profile.email", "address.city"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        let doc = &results[0];

        assert!(doc.get("_id").is_some());
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["profile"]["email"], "alice@example.com");
        assert_eq!(doc["address"]["city"], "NYC");

        // profile.age and address.zip should not be present
        assert!(doc["profile"].get("age").is_none());
        assert!(doc["address"].get("zip").is_none());

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_exclude_nested() {
        let path = "/tmp/test_projection_exclude_nested.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({
            "name": "Alice",
            "profile": {
                "age": 30,
                "email": "alice@example.com",
                "phone": "555-1234"
            }
        })).unwrap();

        let results = coll.query()
            .exclude(&["profile.email", "profile.phone"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        let doc = &results[0];

        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["profile"]["age"], 30);
        assert!(doc["profile"].get("email").is_none());
        assert!(doc["profile"].get("phone").is_none());

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_with_filter_and_sort() {
        let path = "/tmp/test_projection_filter_sort.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30, "city": "NYC", "score": 90})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25, "city": "LA", "score": 85})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35, "city": "NYC", "score": 95})).unwrap();

        let results = coll.query()
            .filter("city is \"NYC\"")
            .sort_by("score", SortOrder::Desc)
            .project(&["name", "score"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);

        // Check first result
        assert_eq!(results[0]["name"], "Charlie");
        assert_eq!(results[0]["score"], 95);
        assert!(results[0].get("age").is_none());
        assert!(results[0].get("city").is_none());

        // Check second result
        assert_eq!(results[1]["name"], "Alice");
        assert_eq!(results[1]["score"], 90);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_exclude_id() {
        let path = "/tmp/test_projection_exclude_id.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();

        let results = coll.query()
            .exclude(&["_id"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        let doc = &results[0];

        assert!(doc.get("_id").is_none());
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_empty_document() {
        let path = "/tmp/test_projection_empty.db";
        let (db, coll) = setup_test_db(path);

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();

        // Project fields that don't exist
        let results = coll.query()
            .project(&["nonexistent", "alsonothere"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        let doc = &results[0];

        // Should only have _id
        assert!(doc.get("_id").is_some());
        assert!(doc.get("name").is_none());
        assert!(doc.get("age").is_none());

        cleanup_test_db(path, db);
    }

    #[test]
    fn test_projection_with_limit() {
        let path = "/tmp/test_projection_limit.db";
        let (db, coll) = setup_test_db(path);

        for i in 1..=10 {
            coll.insert(json!({
                "name": format!("User{}", i),
                "age": 20 + i,
                "email": format!("user{}@example.com", i)
            })).unwrap();
        }

        let results = coll.query()
            .sort_by("age", SortOrder::Asc)
            .limit(5)
            .project(&["name"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 5);

        for doc in &results {
            assert!(doc.get("_id").is_some());
            assert!(doc.get("name").is_some());
            assert!(doc.get("age").is_none());
            assert!(doc.get("email").is_none());
        }

        cleanup_test_db(path, db);
    }
}
