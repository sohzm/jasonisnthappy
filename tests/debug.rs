// Debug and profiling tests - for manual performance analysis
// Not run by default in CI

#[path = "debug/commit_profiling.rs"]
mod commit_profiling;

#[path = "debug/clone_overhead.rs"]
mod clone_overhead;

#[path = "debug/deadlock_scenarios.rs"]
mod deadlock_scenarios;

#[path = "debug/btree_pure.rs"]
mod btree_pure;
