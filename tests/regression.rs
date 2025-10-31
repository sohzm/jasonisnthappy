// Regression tests for previously fixed bugs

#[path = "regression/btree_split.rs"]
mod btree_split;

#[path = "regression/batch_commit.rs"]
mod batch_commit;

#[path = "regression/btree_degradation.rs"]
mod btree_degradation;
