use clap::{Parser, Subcommand};
use anyhow::Result;

mod commands;
mod formatter;
mod repl;
mod utils;

use commands::CommandContext;

#[derive(Parser)]
#[command(name = "jasonisnthappy")]
#[command(author, version, about = "CLI for jasonisnthappy document database", long_about = None)]
struct Cli {
    /// Path to the database file
    #[arg(value_name = "DATABASE")]
    database: Option<String>,

    /// Start interactive REPL mode
    #[arg(short, long)]
    interactive: bool,

    /// Start web UI server
    #[arg(short, long)]
    web_ui: bool,

    /// Web UI address (default: 127.0.0.1:8080)
    #[arg(long, default_value = "127.0.0.1:8080")]
    web_address: String,

    /// Output format: json, table, pretty
    #[arg(short, long, default_value = "pretty")]
    format: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Database operations
    #[command(subcommand)]
    Db(DbCommands),

    /// Collection operations
    #[command(subcommand)]
    Collection(CollectionCommands),

    /// Document operations
    #[command(subcommand)]
    Doc(DocumentCommands),

    /// Query operations
    #[command(subcommand)]
    Query(QueryCommands),

    /// Index operations
    #[command(subcommand)]
    Index(IndexCommands),

    /// Schema operations
    #[command(subcommand)]
    Schema(SchemaCommands),

    /// Monitoring and metrics
    Metrics,

    /// Watch for changes in a collection
    Watch {
        /// Collection name
        collection: String,
    },

    /// Export collection to JSON file
    Export {
        /// Collection name
        collection: String,
        /// Output file path
        output: String,
    },

    /// Import collection from JSON file
    Import {
        /// Collection name
        collection: String,
        /// Input file path
        input: String,
    },
}

#[derive(Subcommand)]
enum DbCommands {
    /// Show database information
    Info,

    /// Create a backup of the database
    Backup {
        /// Destination path for backup
        destination: String,
    },

    /// Compact the database
    Compact,

    /// List all collections
    Collections,
}

#[derive(Subcommand)]
enum CollectionCommands {
    /// List all collections
    List,

    /// Create a new collection
    Create {
        /// Collection name
        name: String,
    },

    /// Drop a collection
    Drop {
        /// Collection name
        name: String,
    },

    /// Show collection information
    Info {
        /// Collection name
        name: String,
    },
}

#[derive(Subcommand)]
enum DocumentCommands {
    /// Insert a document
    Insert {
        /// Collection name
        collection: String,
        /// JSON document to insert
        document: String,
    },

    /// Find documents
    Find {
        /// Collection name
        collection: String,
        /// Query filter (JSON)
        #[arg(default_value = "{}")]
        query: String,
        /// Limit number of results
        #[arg(short, long)]
        limit: Option<usize>,
        /// Skip number of results
        #[arg(short, long)]
        skip: Option<usize>,
    },

    /// Find a single document
    FindOne {
        /// Collection name
        collection: String,
        /// Query filter (JSON)
        query: String,
    },

    /// Update documents
    Update {
        /// Collection name
        collection: String,
        /// Query filter (JSON)
        query: String,
        /// Update operations (JSON)
        update: String,
    },

    /// Delete documents
    Delete {
        /// Collection name
        collection: String,
        /// Query filter (JSON)
        query: String,
    },

    /// Count documents
    Count {
        /// Collection name
        collection: String,
        /// Query filter (JSON)
        #[arg(default_value = "{}")]
        query: String,
    },
}

#[derive(Subcommand)]
enum QueryCommands {
    /// Run a query with filters and sorting
    Run {
        /// Collection name
        collection: String,
        /// Filter (JSON)
        #[arg(short, long, default_value = "{}")]
        filter: String,
        /// Sort field
        #[arg(short, long)]
        sort: Option<String>,
        /// Sort order: asc or desc
        #[arg(long, default_value = "asc")]
        order: String,
        /// Limit
        #[arg(short, long)]
        limit: Option<usize>,
        /// Skip
        #[arg(long)]
        skip: Option<usize>,
    },

    /// Run an aggregation pipeline
    Aggregate {
        /// Collection name
        collection: String,
        /// Aggregation pipeline (JSON array)
        pipeline: String,
    },

    /// Full-text search
    Search {
        /// Collection name
        collection: String,
        /// Search text
        text: String,
        /// Limit results
        #[arg(short, long)]
        limit: Option<usize>,
    },
}

#[derive(Subcommand)]
enum IndexCommands {
    /// List indexes for a collection
    List {
        /// Collection name
        collection: String,
    },

    /// Create an index
    Create {
        /// Collection name
        collection: String,
        /// Field name
        field: String,
        /// Make it unique
        #[arg(short, long)]
        unique: bool,
    },

    /// Drop an index
    Drop {
        /// Collection name
        collection: String,
        /// Index name
        name: String,
    },
}

#[derive(Subcommand)]
enum SchemaCommands {
    /// Set schema for a collection
    Set {
        /// Collection name
        collection: String,
        /// Schema definition (JSON)
        schema: String,
    },

    /// Get schema for a collection
    Get {
        /// Collection name
        collection: String,
    },

    /// Validate all documents against schema
    Validate {
        /// Collection name
        collection: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // If interactive mode or no command specified, start REPL
    if cli.interactive || (cli.command.is_none() && cli.database.is_some()) {
        let db_path = cli.database.as_deref().unwrap_or("data.db");
        return repl::start(db_path, &cli.format, cli.web_ui, &cli.web_address);
    }

    // Require database path for non-interactive commands
    let db_path = cli.database.as_deref().ok_or_else(|| {
        anyhow::anyhow!("Database path required. Usage: jasonisnthappy <DATABASE> [COMMAND]")
    })?;

    // Create command context
    let mut ctx = CommandContext::new(db_path, &cli.format)?;

    // Start web UI if requested
    if cli.web_ui {
        ctx.start_web_ui(&cli.web_address)?;
    }

    // Execute command
    if let Some(command) = cli.command {
        execute_command(&mut ctx, command)?;
    } else {
        // No command specified, show info
        commands::db::info(&ctx)?;
    }

    Ok(())
}

fn execute_command(ctx: &mut CommandContext, command: Commands) -> Result<()> {
    match command {
        Commands::Db(cmd) => match cmd {
            DbCommands::Info => commands::db::info(ctx),
            DbCommands::Backup { destination } => commands::db::backup(ctx, &destination),
            DbCommands::Compact => commands::db::compact(ctx),
            DbCommands::Collections => commands::collection::list(ctx),
        },
        Commands::Collection(cmd) => match cmd {
            CollectionCommands::List => commands::collection::list(ctx),
            CollectionCommands::Create { name } => commands::collection::create(ctx, &name),
            CollectionCommands::Drop { name } => commands::collection::drop(ctx, &name),
            CollectionCommands::Info { name } => commands::collection::info(ctx, &name),
        },
        Commands::Doc(cmd) => match cmd {
            DocumentCommands::Insert { collection, document } => {
                commands::document::insert(ctx, &collection, &document)
            }
            DocumentCommands::Find { collection, query, limit, skip } => {
                commands::document::find(ctx, &collection, &query, limit, skip)
            }
            DocumentCommands::FindOne { collection, query } => {
                commands::document::find_one(ctx, &collection, &query)
            }
            DocumentCommands::Update { collection, query, update } => {
                commands::document::update(ctx, &collection, &query, &update)
            }
            DocumentCommands::Delete { collection, query } => {
                commands::document::delete(ctx, &collection, &query)
            }
            DocumentCommands::Count { collection, query } => {
                commands::document::count(ctx, &collection, &query)
            }
        },
        Commands::Query(cmd) => match cmd {
            QueryCommands::Run { collection, filter, sort, order, limit, skip } => {
                commands::query::run(ctx, &collection, &filter, sort.as_deref(), &order, limit, skip)
            }
            QueryCommands::Aggregate { collection, pipeline } => {
                commands::query::aggregate(ctx, &collection, &pipeline)
            }
            QueryCommands::Search { collection, text, limit } => {
                commands::query::search(ctx, &collection, &text, limit)
            }
        },
        Commands::Index(cmd) => match cmd {
            IndexCommands::List { collection } => commands::index::list(ctx, &collection),
            IndexCommands::Create { collection, field, unique } => {
                commands::index::create(ctx, &collection, &field, unique)
            }
            IndexCommands::Drop { collection, name } => {
                commands::index::drop(ctx, &collection, &name)
            }
        },
        Commands::Schema(cmd) => match cmd {
            SchemaCommands::Set { collection, schema } => {
                commands::schema::set(ctx, &collection, &schema)
            }
            SchemaCommands::Get { collection } => commands::schema::get(ctx, &collection),
            SchemaCommands::Validate { collection } => {
                commands::schema::validate(ctx, &collection)
            }
        },
        Commands::Metrics => commands::metrics::show(ctx),
        Commands::Watch { collection } => commands::metrics::watch(ctx, &collection),
        Commands::Export { collection, output } => {
            commands::document::export(ctx, &collection, &output)
        }
        Commands::Import { collection, input } => {
            commands::document::import(ctx, &collection, &input)
        }
    }
}
