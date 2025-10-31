use crate::core::collection::Collection;
use crate::core::errors::*;
use crate::core::query::parser::parse_query;
use serde_json::{json, Value};
use std::collections::HashMap;

/// A stage in an aggregation pipeline
#[derive(Debug, Clone)]
enum Stage {
    /// Filter documents (like find query)
    Match(String),
    /// Group documents by a field and apply aggregation functions
    GroupBy {
        field: String,
        accumulators: Vec<Accumulator>,
    },
    /// Sort results by a field
    Sort { field: String, ascending: bool },
    /// Limit number of results
    Limit(usize),
    /// Skip a number of results
    Skip(usize),
    /// Select specific fields to include/exclude
    Project { fields: Vec<String>, exclude: bool },
}

/// An accumulator function for group operations
#[derive(Debug, Clone)]
struct Accumulator {
    /// Name of the output field
    output_field: String,
    /// Type of accumulation
    op: AccumulatorOp,
}

/// Types of accumulator operations
#[derive(Debug, Clone)]
enum AccumulatorOp {
    Count,
    Sum(String),  // field to sum
    Avg(String),  // field to average
    Min(String),  // field to get minimum
    Max(String),  // field to get maximum
}

/// Builder for aggregation pipelines
pub struct AggregationPipeline<'a> {
    collection: &'a Collection,
    stages: Vec<Stage>,
}

impl<'a> AggregationPipeline<'a> {
    pub(crate) fn new(collection: &'a Collection) -> Self {
        Self {
            collection,
            stages: Vec::new(),
        }
    }

    /// Add a match stage to filter documents
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.match_("age > 25")
    /// # ;
    /// ```
    pub fn match_(mut self, query: &str) -> Self {
        self.stages.push(Stage::Match(query.to_string()));
        self
    }

    /// Add a group by stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.group_by("city")
    /// # ;
    /// ```
    pub fn group_by(mut self, field: &str) -> Self {
        self.stages.push(Stage::GroupBy {
            field: field.to_string(),
            accumulators: Vec::new(),
        });
        self
    }

    /// Add a count accumulator to the last group by stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.group_by("city").count("total")
    /// # ;
    /// ```
    pub fn count(mut self, output_field: &str) -> Self {
        if let Some(Stage::GroupBy { accumulators, .. }) = self.stages.last_mut() {
            accumulators.push(Accumulator {
                output_field: output_field.to_string(),
                op: AccumulatorOp::Count,
            });
        }
        self
    }

    /// Add a sum accumulator to the last group by stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.group_by("city").sum("age", "total_age")
    /// # ;
    /// ```
    pub fn sum(mut self, field: &str, output_field: &str) -> Self {
        if let Some(Stage::GroupBy { accumulators, .. }) = self.stages.last_mut() {
            accumulators.push(Accumulator {
                output_field: output_field.to_string(),
                op: AccumulatorOp::Sum(field.to_string()),
            });
        }
        self
    }

    /// Add an average accumulator to the last group by stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.group_by("city").avg("age", "avg_age")
    /// # ;
    /// ```
    pub fn avg(mut self, field: &str, output_field: &str) -> Self {
        if let Some(Stage::GroupBy { accumulators, .. }) = self.stages.last_mut() {
            accumulators.push(Accumulator {
                output_field: output_field.to_string(),
                op: AccumulatorOp::Avg(field.to_string()),
            });
        }
        self
    }

    /// Add a min accumulator to the last group by stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.group_by("city").min("age", "min_age")
    /// # ;
    /// ```
    pub fn min(mut self, field: &str, output_field: &str) -> Self {
        if let Some(Stage::GroupBy { accumulators, .. }) = self.stages.last_mut() {
            accumulators.push(Accumulator {
                output_field: output_field.to_string(),
                op: AccumulatorOp::Min(field.to_string()),
            });
        }
        self
    }

    /// Add a max accumulator to the last group by stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.group_by("city").max("age", "max_age")
    /// # ;
    /// ```
    pub fn max(mut self, field: &str, output_field: &str) -> Self {
        if let Some(Stage::GroupBy { accumulators, .. }) = self.stages.last_mut() {
            accumulators.push(Accumulator {
                output_field: output_field.to_string(),
                op: AccumulatorOp::Max(field.to_string()),
            });
        }
        self
    }

    /// Add a sort stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.sort("age", true)  // ascending
    /// # ;
    /// ```
    pub fn sort(mut self, field: &str, ascending: bool) -> Self {
        self.stages.push(Stage::Sort {
            field: field.to_string(),
            ascending,
        });
        self
    }

    /// Add a limit stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.limit(10)
    /// # ;
    /// ```
    pub fn limit(mut self, n: usize) -> Self {
        self.stages.push(Stage::Limit(n));
        self
    }

    /// Add a skip stage
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.skip(5)
    /// # ;
    /// ```
    pub fn skip(mut self, n: usize) -> Self {
        self.stages.push(Stage::Skip(n));
        self
    }

    /// Add a project stage to include specific fields
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.project(&["name", "age"])
    /// # ;
    /// ```
    pub fn project(mut self, fields: &[&str]) -> Self {
        self.stages.push(Stage::Project {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            exclude: false,
        });
        self
    }

    /// Add a project stage to exclude specific fields
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// # let collection = db.collection("users");
    /// # let pipeline = collection.aggregate();
    /// pipeline.exclude(&["password", "secret"])
    /// # ;
    /// ```
    pub fn exclude(mut self, fields: &[&str]) -> Self {
        self.stages.push(Stage::Project {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            exclude: true,
        });
        self
    }

    /// Execute the aggregation pipeline and return results
    pub fn execute(self) -> Result<Vec<Value>> {
        // Start with all documents in the collection
        let mut documents = match self.collection.find_all() {
            Ok(docs) => docs,
            Err(Error::Other(msg)) if msg.contains("not found") => Vec::new(),
            Err(e) => return Err(e),
        };

        // Execute each stage in sequence
        for stage in &self.stages {
            documents = self.execute_stage(stage, documents)?;
        }

        Ok(documents)
    }

    /// Execute a single stage of the pipeline
    fn execute_stage(&self, stage: &Stage, documents: Vec<Value>) -> Result<Vec<Value>> {
        match stage {
            Stage::Match(query) => self.execute_match(&query, documents),
            Stage::GroupBy { field, accumulators } => {
                self.execute_group_by(&field, &accumulators, documents)
            }
            Stage::Sort { field, ascending } => {
                self.execute_sort(&field, *ascending, documents)
            }
            Stage::Limit(n) => Ok(documents.into_iter().take(*n).collect()),
            Stage::Skip(n) => Ok(documents.into_iter().skip(*n).collect()),
            Stage::Project { fields, exclude } => {
                self.execute_project(&fields, *exclude, documents)
            }
        }
    }

    /// Execute a match stage
    fn execute_match(&self, query: &str, documents: Vec<Value>) -> Result<Vec<Value>> {
        let ast = parse_query(query)
            .map_err(|e| Error::Other(format!("failed to parse query: {}", e)))?;

        Ok(documents
            .into_iter()
            .filter(|doc| {
                if let Some(doc_map) = doc.as_object() {
                    ast.eval(doc_map)
                } else {
                    false
                }
            })
            .collect())
    }

    /// Execute a group by stage
    fn execute_group_by(
        &self,
        field: &str,
        accumulators: &[Accumulator],
        documents: Vec<Value>,
    ) -> Result<Vec<Value>> {
        // Group documents by the field value
        let mut groups: HashMap<String, Vec<Value>> = HashMap::new();

        for doc in documents {
            let key = match doc.get(field) {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Bool(b)) => b.to_string(),
                Some(Value::Null) => "null".to_string(),
                None => "null".to_string(),
                Some(other) => other.to_string(),
            };

            groups.entry(key).or_insert_with(Vec::new).push(doc);
        }

        // Apply accumulators to each group
        let mut results = Vec::new();
        for (key, group_docs) in groups {
            let mut result = json!({
                "_id": key,
            });

            let result_obj = result.as_object_mut()
                .ok_or_else(|| Error::Other("aggregation result must be an object".to_string()))?;

            for accumulator in accumulators {
                let value = match &accumulator.op {
                    AccumulatorOp::Count => {
                        Value::Number(group_docs.len().into())
                    }
                    AccumulatorOp::Sum(sum_field) => {
                        let sum: f64 = group_docs
                            .iter()
                            .filter_map(|doc| doc.get(sum_field))
                            .filter_map(|v| v.as_f64())
                            .sum();
                        json!(sum)
                    }
                    AccumulatorOp::Avg(avg_field) => {
                        let values: Vec<f64> = group_docs
                            .iter()
                            .filter_map(|doc| doc.get(avg_field))
                            .filter_map(|v| v.as_f64())
                            .collect();

                        if values.is_empty() {
                            Value::Null
                        } else {
                            let avg = values.iter().sum::<f64>() / values.len() as f64;
                            json!(avg)
                        }
                    }
                    AccumulatorOp::Min(min_field) => {
                        group_docs
                            .iter()
                            .filter_map(|doc| doc.get(min_field))
                            .filter_map(|v| v.as_f64())
                            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                            .map(|v| json!(v))
                            .unwrap_or(Value::Null)
                    }
                    AccumulatorOp::Max(max_field) => {
                        group_docs
                            .iter()
                            .filter_map(|doc| doc.get(max_field))
                            .filter_map(|v| v.as_f64())
                            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                            .map(|v| json!(v))
                            .unwrap_or(Value::Null)
                    }
                };

                result_obj.insert(accumulator.output_field.clone(), value);
            }

            results.push(result);
        }

        Ok(results)
    }

    /// Execute a sort stage
    fn execute_sort(
        &self,
        field: &str,
        ascending: bool,
        mut documents: Vec<Value>,
    ) -> Result<Vec<Value>> {
        documents.sort_by(|a, b| {
            let a_val = a.get(field);
            let b_val = b.get(field);

            let cmp = match (a_val, b_val) {
                (Some(Value::String(a)), Some(Value::String(b))) => a.cmp(b),
                (Some(Value::Number(a)), Some(Value::Number(b))) => {
                    let a_f64 = a.as_f64().unwrap_or(0.0);
                    let b_f64 = b.as_f64().unwrap_or(0.0);
                    a_f64.partial_cmp(&b_f64).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Some(Value::Bool(a)), Some(Value::Bool(b))) => a.cmp(b),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                _ => std::cmp::Ordering::Equal,
            };

            if ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });

        Ok(documents)
    }

    /// Execute a project stage
    fn execute_project(
        &self,
        fields: &[String],
        exclude: bool,
        documents: Vec<Value>,
    ) -> Result<Vec<Value>> {
        Ok(documents
            .into_iter()
            .map(|doc| {
                if let Some(obj) = doc.as_object() {
                    let mut new_obj = serde_json::Map::new();

                    if exclude {
                        // Include all fields except the specified ones
                        for (key, value) in obj {
                            if !fields.contains(key) {
                                new_obj.insert(key.clone(), value.clone());
                            }
                        }
                    } else {
                        // Include only the specified fields
                        for field in fields {
                            if let Some(value) = obj.get(field) {
                                new_obj.insert(field.clone(), value.clone());
                            }
                        }
                        // Always include _id unless explicitly excluded
                        if !fields.contains(&"_id".to_string()) {
                            if let Some(id) = obj.get("_id") {
                                new_obj.insert("_id".to_string(), id.clone());
                            }
                        }
                    }

                    Value::Object(new_obj)
                } else {
                    doc
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use serde_json::json;

    #[test]
    fn test_match_stage() {
        let path = "/tmp/test_agg_match.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"name": "Alice", "age": 30})).unwrap();
        users.insert(json!({"name": "Bob", "age": 25})).unwrap();
        users.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let results = users.aggregate().match_("age > 28").execute().unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.get("name").unwrap() == "Alice"));
        assert!(results.iter().any(|r| r.get("name").unwrap() == "Charlie"));
    }

    #[test]
    fn test_group_by_count() {
        let path = "/tmp/test_agg_group_count.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"name": "Alice", "city": "NYC"})).unwrap();
        users.insert(json!({"name": "Bob", "city": "LA"})).unwrap();
        users.insert(json!({"name": "Charlie", "city": "NYC"})).unwrap();

        let results = users
            .aggregate()
            .group_by("city")
            .count("total")
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);

        let nyc = results.iter().find(|r| r.get("_id").unwrap() == "NYC").unwrap();
        assert_eq!(nyc.get("total").unwrap(), 2);

        let la = results.iter().find(|r| r.get("_id").unwrap() == "LA").unwrap();
        assert_eq!(la.get("total").unwrap(), 1);
    }

    #[test]
    fn test_group_by_sum() {
        let path = "/tmp/test_agg_group_sum.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let sales = db.collection("sales");

        sales.insert(json!({"item": "apple", "price": 10.0})).unwrap();
        sales.insert(json!({"item": "banana", "price": 5.0})).unwrap();
        sales.insert(json!({"item": "apple", "price": 15.0})).unwrap();

        let results = sales
            .aggregate()
            .group_by("item")
            .sum("price", "total_price")
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);

        let apple = results.iter().find(|r| r.get("_id").unwrap() == "apple").unwrap();
        assert_eq!(apple.get("total_price").unwrap(), 25.0);

        let banana = results.iter().find(|r| r.get("_id").unwrap() == "banana").unwrap();
        assert_eq!(banana.get("total_price").unwrap(), 5.0);
    }

    #[test]
    fn test_group_by_avg() {
        let path = "/tmp/test_agg_group_avg.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"city": "NYC", "age": 30})).unwrap();
        users.insert(json!({"city": "NYC", "age": 40})).unwrap();
        users.insert(json!({"city": "LA", "age": 25})).unwrap();

        let results = users
            .aggregate()
            .group_by("city")
            .avg("age", "avg_age")
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);

        let nyc = results.iter().find(|r| r.get("_id").unwrap() == "NYC").unwrap();
        assert_eq!(nyc.get("avg_age").unwrap(), 35.0);

        let la = results.iter().find(|r| r.get("_id").unwrap() == "LA").unwrap();
        assert_eq!(la.get("avg_age").unwrap(), 25.0);
    }

    #[test]
    fn test_group_by_min_max() {
        let path = "/tmp/test_agg_group_minmax.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"city": "NYC", "age": 30})).unwrap();
        users.insert(json!({"city": "NYC", "age": 40})).unwrap();
        users.insert(json!({"city": "NYC", "age": 20})).unwrap();

        let results = users
            .aggregate()
            .group_by("city")
            .min("age", "min_age")
            .max("age", "max_age")
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);

        let nyc = &results[0];
        assert_eq!(nyc.get("min_age").unwrap(), 20.0);
        assert_eq!(nyc.get("max_age").unwrap(), 40.0);
    }

    #[test]
    fn test_multi_stage_pipeline() {
        let path = "/tmp/test_agg_multi_stage.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"name": "Alice", "city": "NYC", "age": 30})).unwrap();
        users.insert(json!({"name": "Bob", "city": "LA", "age": 25})).unwrap();
        users.insert(json!({"name": "Charlie", "city": "NYC", "age": 35})).unwrap();
        users.insert(json!({"name": "David", "city": "NYC", "age": 20})).unwrap();

        let results = users
            .aggregate()
            .match_("age > 22")
            .group_by("city")
            .count("total")
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);

        let nyc = results.iter().find(|r| r.get("_id").unwrap() == "NYC").unwrap();
        assert_eq!(nyc.get("total").unwrap(), 2); // Alice and Charlie (David filtered out)

        let la = results.iter().find(|r| r.get("_id").unwrap() == "LA").unwrap();
        assert_eq!(la.get("total").unwrap(), 1);
    }

    #[test]
    fn test_sort_stage() {
        let path = "/tmp/test_agg_sort.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"name": "Charlie", "age": 35})).unwrap();
        users.insert(json!({"name": "Alice", "age": 30})).unwrap();
        users.insert(json!({"name": "Bob", "age": 25})).unwrap();

        let results = users.aggregate().sort("age", true).execute().unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("name").unwrap(), "Bob");
        assert_eq!(results[1].get("name").unwrap(), "Alice");
        assert_eq!(results[2].get("name").unwrap(), "Charlie");
    }

    #[test]
    fn test_limit_skip_stages() {
        let path = "/tmp/test_agg_limit_skip.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"name": "Alice", "age": 30})).unwrap();
        users.insert(json!({"name": "Bob", "age": 25})).unwrap();
        users.insert(json!({"name": "Charlie", "age": 35})).unwrap();
        users.insert(json!({"name": "David", "age": 28})).unwrap();

        let results = users
            .aggregate()
            .sort("age", true)
            .skip(1)
            .limit(2)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("name").unwrap(), "David");
        assert_eq!(results[1].get("name").unwrap(), "Alice");
    }

    #[test]
    fn test_project_stage() {
        let path = "/tmp/test_agg_project.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"name": "Alice", "age": 30, "city": "NYC"})).unwrap();

        let results = users
            .aggregate()
            .project(&["name", "age"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].get("name").is_some());
        assert!(results[0].get("age").is_some());
        assert!(results[0].get("city").is_none());
        assert!(results[0].get("_id").is_some()); // _id always included
    }

    #[test]
    fn test_exclude_stage() {
        let path = "/tmp/test_agg_exclude.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        users.insert(json!({"name": "Alice", "age": 30, "password": "secret"})).unwrap();

        let results = users
            .aggregate()
            .exclude(&["password"])
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].get("name").is_some());
        assert!(results[0].get("age").is_some());
        assert!(results[0].get("password").is_none());
    }

    #[test]
    fn test_empty_collection() {
        let path = "/tmp/test_agg_empty.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        let results = users
            .aggregate()
            .group_by("city")
            .count("total")
            .execute()
            .unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_complex_pipeline() {
        let path = "/tmp/test_agg_complex.db";
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let sales = db.collection("sales");

        sales.insert(json!({"product": "laptop", "category": "electronics", "price": 1000.0, "quantity": 2})).unwrap();
        sales.insert(json!({"product": "phone", "category": "electronics", "price": 500.0, "quantity": 3})).unwrap();
        sales.insert(json!({"product": "desk", "category": "furniture", "price": 300.0, "quantity": 1})).unwrap();
        sales.insert(json!({"product": "chair", "category": "furniture", "price": 150.0, "quantity": 4})).unwrap();
        sales.insert(json!({"product": "tablet", "category": "electronics", "price": 400.0, "quantity": 1})).unwrap();

        let results = sales
            .aggregate()
            .match_("price > 200")
            .group_by("category")
            .sum("price", "total_price")
            .count("num_products")
            .sort("total_price", false) // descending
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("_id").unwrap(), "electronics"); // Higher total
        assert_eq!(results[0].get("total_price").unwrap(), 1900.0);
        assert_eq!(results[0].get("num_products").unwrap(), 3);
    }
}
