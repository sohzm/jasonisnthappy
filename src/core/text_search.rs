use crate::core::btree::BTree;
use crate::core::constants::PAGE_SIZE;
use crate::core::errors::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use unicode_segmentation::UnicodeSegmentation;

/// Metadata about a text index stored in collection metadata
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TextIndexMeta {
    /// Name of the text index
    pub name: String,
    /// Fields included in this text index
    pub fields: Vec<String>,
    /// Root page of the inverted index B-tree
    pub btree_root: u64,
}

/// A search result with relevance score
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SearchResult {
    /// Document ID
    pub doc_id: String,
    /// Relevance score (higher = more relevant)
    pub score: f32,
}

impl SearchResult {
    pub fn new(doc_id: String, score: f32) -> Self {
        Self { doc_id, score }
    }
}

/// Inverted index for text search
/// Maps terms to documents and their scores
pub struct TextIndex {
    btree: BTree,
    fields: Vec<String>,
}

impl TextIndex {
    /// Create a new text index with the given B-tree
    pub fn new(btree: BTree, fields: Vec<String>) -> Self {
        Self { btree, fields }
    }

    /// Get a reference to the underlying B-tree
    pub fn btree(&self) -> &BTree {
        &self.btree
    }

    /// Get the indexed fields
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Index a document's text fields
    pub fn index_document(&mut self, doc_id: &str, field_values: &HashMap<String, String>) -> Result<()> {
        for field in &self.fields {
            if let Some(text) = field_values.get(field) {
                let tokens = tokenize(text);
                let term_freq = calculate_term_frequency(&tokens);

                for (term, freq) in term_freq {
                    let key = format!("{}:{}", term, field);

                    // Get existing postings or create new
                    let mut postings: HashMap<String, f32> = match self.btree.search(&key) {
                        Ok(page_num) => {
                            let pager = self.btree.pager();
                            let page = pager.read_page(page_num)?;

                            // Trim null bytes
                            let trimmed = page.iter()
                                .rposition(|&b| b != 0)
                                .map(|pos| &page[..=pos])
                                .unwrap_or(&page[..0]);

                            serde_json::from_slice(trimmed).unwrap_or_default()
                        }
                        Err(_) => HashMap::new(),
                    };

                    // Add or update this document's term frequency
                    postings.insert(doc_id.to_string(), freq);

                    // Serialize and store
                    let data = serde_json::to_vec(&postings)?;
                    let page_num = self.write_data(&data)?;
                    self.btree.insert(&key, page_num)?;
                }
            }
        }

        Ok(())
    }

    /// Remove a document from the index
    pub fn remove_document(&mut self, doc_id: &str, field_values: &HashMap<String, String>) -> Result<()> {
        for field in &self.fields {
            if let Some(text) = field_values.get(field) {
                let tokens = tokenize(text);
                let unique_terms: std::collections::HashSet<_> = tokens.into_iter().collect();

                for term in unique_terms {
                    let key = format!("{}:{}", term, field);

                    if let Ok(page_num) = self.btree.search(&key) {
                        let pager = self.btree.pager();
                        let page = pager.read_page(page_num)?;

                        // Trim null bytes
                        let trimmed = page.iter()
                            .rposition(|&b| b != 0)
                            .map(|pos| &page[..=pos])
                            .unwrap_or(&page[..0]);

                        let mut postings: HashMap<String, f32> =
                            serde_json::from_slice(trimmed).unwrap_or_default();

                        postings.remove(doc_id);

                        if postings.is_empty() {
                            // Remove the term entirely if no documents remain
                            self.btree.delete(&key)?;
                        } else {
                            // Update with remaining documents
                            let data = serde_json::to_vec(&postings)?;
                            let new_page_num = self.write_data(&data)?;
                            self.btree.insert(&key, new_page_num)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Search for documents matching the query
    /// Returns documents sorted by relevance (highest score first)
    pub fn search(&self, query: &str, total_docs: usize) -> Result<Vec<SearchResult>> {
        let query_terms = tokenize(query);
        if query_terms.is_empty() {
            return Ok(Vec::new());
        }

        // Collect document scores for each term
        let mut doc_scores: HashMap<String, f32> = HashMap::new();

        for term in query_terms {
            // Search across all indexed fields
            for field in &self.fields {
                let key = format!("{}:{}", term, field);

                if let Ok(page_num) = self.btree.search(&key) {
                    let pager = self.btree.pager();
                    let page = pager.read_page(page_num)?;

                    // Trim null bytes from the end
                    let trimmed = page.iter()
                        .rposition(|&b| b != 0)
                        .map(|pos| &page[..=pos])
                        .unwrap_or(&page[..0]);

                    let postings: HashMap<String, f32> =
                        serde_json::from_slice(trimmed).unwrap_or_default();

                    // Calculate IDF for this term
                    let idf = calculate_idf(total_docs, postings.len());

                    // Add TF-IDF score for each document
                    for (doc_id, tf) in postings {
                        let score = tf * idf;
                        *doc_scores.entry(doc_id).or_insert(0.0) += score;
                    }
                }
            }
        }

        // Convert to SearchResult and sort by score
        let mut results: Vec<SearchResult> = doc_scores
            .into_iter()
            .map(|(doc_id, score)| SearchResult::new(doc_id, score))
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        Ok(results)
    }

    /// Write data to a new page and return the page number
    fn write_data(&self, data: &[u8]) -> Result<u64> {
        let pager = self.btree.pager();
        let page_num = pager.alloc_page()?;

        // Ensure the data fits in a page
        if data.len() > PAGE_SIZE {
            return Err(Error::Other("text index data too large for page".to_string()));
        }

        let mut page_data = vec![0u8; PAGE_SIZE];
        page_data[..data.len()].copy_from_slice(data);
        pager.write_page(page_num, &page_data)?;

        Ok(page_num)
    }
}

/// Tokenize text into terms (lowercase words)
pub fn tokenize(text: &str) -> Vec<String> {
    text.unicode_words()
        .map(|word| word.to_lowercase())
        .filter(|word| word.len() > 1) // Filter out single-character terms
        .collect()
}

/// Calculate term frequency for a list of tokens
/// TF = count of term / total number of terms
fn calculate_term_frequency(tokens: &[String]) -> HashMap<String, f32> {
    let total = tokens.len() as f32;
    let mut counts: HashMap<String, f32> = HashMap::new();

    for token in tokens {
        *counts.entry(token.clone()).or_insert(0.0) += 1.0;
    }

    // Normalize by total count
    for count in counts.values_mut() {
        *count /= total;
    }

    counts
}

/// Calculate inverse document frequency
/// IDF = log(total_documents / documents_containing_term)
fn calculate_idf(total_docs: usize, docs_with_term: usize) -> f32 {
    if docs_with_term == 0 {
        return 0.0;
    }
    ((total_docs as f32) / (docs_with_term as f32)).ln()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        let text = "Hello, World! This is a test.";
        let tokens = tokenize(text);
        assert_eq!(tokens, vec!["hello", "world", "this", "is", "test"]);
    }

    #[test]
    fn test_tokenize_unicode() {
        let text = "Rust is 🔥 amazing!";
        let tokens = tokenize(text);
        assert_eq!(tokens, vec!["rust", "is", "amazing"]);
    }

    #[test]
    fn test_tokenize_filters_single_chars() {
        let text = "a b cd e fg";
        let tokens = tokenize(text);
        assert_eq!(tokens, vec!["cd", "fg"]);
    }

    #[test]
    fn test_term_frequency() {
        let tokens = vec![
            "rust".to_string(),
            "database".to_string(),
            "rust".to_string(),
            "test".to_string(),
        ];
        let tf = calculate_term_frequency(&tokens);

        assert_eq!(tf.get("rust"), Some(&0.5)); // 2/4
        assert_eq!(tf.get("database"), Some(&0.25)); // 1/4
        assert_eq!(tf.get("test"), Some(&0.25)); // 1/4
    }

    #[test]
    fn test_idf() {
        let idf1 = calculate_idf(100, 10);
        let idf2 = calculate_idf(100, 50);

        // More common terms should have lower IDF
        assert!(idf1 > idf2);

        // IDF should be positive
        assert!(idf1 > 0.0);
        assert!(idf2 > 0.0);
    }

    #[test]
    fn test_idf_zero_docs() {
        let idf = calculate_idf(100, 0);
        assert_eq!(idf, 0.0);
    }
}
