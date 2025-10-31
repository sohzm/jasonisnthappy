use anyhow::Result;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result as RustyResult};
use colored::Colorize;
use crate::commands::CommandContext;
use crate::formatter::{print_success, print_error, print_info};

pub fn start(db_path: &str, format: &str, web_ui: bool, web_address: &str) -> Result<()> {
    println!("{}", "jasonisnthappy interactive shell".bright_cyan().bold());
    println!("Database: {}", db_path);
    println!("Type 'help' for available commands, 'exit' to quit\n");

    let mut ctx = CommandContext::new(db_path, format)?;

    // Start web UI if requested
    if web_ui {
        ctx.start_web_ui(web_address)?;
    }

    let mut rl = DefaultEditor::new()?;
    let mut current_collection: Option<String> = None;

    loop {
        let prompt = if let Some(ref coll) = current_collection {
            format!("{}> ", coll.bright_green())
        } else {
            "db> ".to_string()
        };

        let readline = rl.readline(&prompt);

        match readline {
            Ok(line) => {
                let line = line.trim();

                if line.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line);

                if let Err(e) = execute_repl_command(&mut ctx, line, &mut current_collection) {
                    print_error(&format!("{}", e));
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("exit");
                break;
            }
            Err(err) => {
                print_error(&format!("Error: {}", err));
                break;
            }
        }
    }

    Ok(())
}

fn execute_repl_command(
    ctx: &mut CommandContext,
    line: &str,
    current_collection: &mut Option<String>,
) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.is_empty() {
        return Ok(());
    }

    match parts[0] {
        "help" => show_help(),
        "exit" | "quit" => std::process::exit(0),
        "info" => crate::commands::db::info(ctx)?,
        "collections" | "show collections" => crate::commands::collection::list(ctx)?,
        "metrics" => crate::commands::metrics::show(ctx)?,

        "use" => {
            if parts.len() < 2 {
                print_error("Usage: use <collection>");
            } else {
                *current_collection = Some(parts[1].to_string());
                print_success(&format!("Now using collection '{}'", parts[1]));
            }
        }

        "create" => {
            if parts.len() < 2 {
                print_error("Usage: create <collection>");
            } else {
                crate::commands::collection::create(ctx, parts[1])?;
            }
        }

        "drop" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else if let Some(ref coll) = current_collection {
                crate::commands::collection::drop(ctx, coll)?;
                *current_collection = None;
            }
        }

        "insert" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else {
                let json_start = line.find('{').ok_or_else(|| {
                    anyhow::anyhow!("Usage: insert {{\"field\": \"value\", ...}}")
                })?;
                let doc_json = &line[json_start..];

                if let Some(ref coll) = current_collection {
                    crate::commands::document::insert(ctx, coll, doc_json)?;
                }
            }
        }

        "find" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else {
                let query = if parts.len() > 1 {
                    let json_start = line.find('{').unwrap_or(line.len());
                    if json_start < line.len() {
                        &line[json_start..]
                    } else {
                        "{}"
                    }
                } else {
                    "{}"
                };

                if let Some(ref coll) = current_collection {
                    crate::commands::document::find(ctx, coll, query, None, None)?;
                }
            }
        }

        "count" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else {
                let query = if parts.len() > 1 {
                    let json_start = line.find('{').unwrap_or(line.len());
                    if json_start < line.len() {
                        &line[json_start..]
                    } else {
                        "{}"
                    }
                } else {
                    "{}"
                };

                if let Some(ref coll) = current_collection {
                    crate::commands::document::count(ctx, coll, query)?;
                }
            }
        }

        "update" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else {
                // Parse: update {query} {update}
                let parts: Vec<&str> = line.split('{').collect();
                if parts.len() < 3 {
                    print_error("Usage: update {{query}} {{update}}");
                } else {
                    let query = format!("{{{}", parts[1]);
                    let update_str = format!("{{{}", parts[2].trim_end_matches('}'));

                    if let Some(ref coll) = current_collection {
                        crate::commands::document::update(ctx, coll, &query, &update_str)?;
                    }
                }
            }
        }

        "delete" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else {
                let query = if parts.len() > 1 {
                    let json_start = line.find('{').unwrap_or(line.len());
                    if json_start < line.len() {
                        &line[json_start..]
                    } else {
                        "{}"
                    }
                } else {
                    "{}"
                };

                if let Some(ref coll) = current_collection {
                    crate::commands::document::delete(ctx, coll, query)?;
                }
            }
        }

        "indexes" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else if let Some(ref coll) = current_collection {
                crate::commands::index::list(ctx, coll)?;
            }
        }

        "create-index" | "createIndex" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else if parts.len() < 2 {
                print_error("Usage: create-index <field> [--unique]");
            } else {
                let field = parts[1];
                let unique = parts.contains(&"--unique");

                if let Some(ref coll) = current_collection {
                    crate::commands::index::create(ctx, coll, field, unique)?;
                }
            }
        }

        "backup" => {
            if parts.len() < 2 {
                print_error("Usage: backup <destination>");
            } else {
                crate::commands::db::backup(ctx, parts[1])?;
            }
        }

        "export" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else if parts.len() < 2 {
                print_error("Usage: export <file>");
            } else if let Some(ref coll) = current_collection {
                crate::commands::document::export(ctx, coll, parts[1])?;
            }
        }

        "import" => {
            if current_collection.is_none() {
                print_error("No collection selected. Use 'use <collection>' first");
            } else if parts.len() < 2 {
                print_error("Usage: import <file>");
            } else if let Some(ref coll) = current_collection {
                crate::commands::document::import(ctx, coll, parts[1])?;
            }
        }

        _ => {
            print_error(&format!("Unknown command: '{}'. Type 'help' for available commands", parts[0]));
        }
    }

    Ok(())
}

fn show_help() {
    println!("\n{}", "Available Commands:".bright_cyan().bold());
    println!();
    println!("{}", "  Database Commands:".bright_yellow());
    println!("    info                    - Show database information");
    println!("    collections             - List all collections");
    println!("    metrics                 - Show database metrics");
    println!("    backup <dest>           - Create database backup");
    println!();
    println!("{}", "  Collection Commands:".bright_yellow());
    println!("    use <collection>        - Select a collection to work with");
    println!("    create <collection>     - Create a new collection");
    println!("    drop                    - Drop the current collection");
    println!();
    println!("{}", "  Document Commands:".bright_yellow());
    println!("    insert {{...}}           - Insert a document");
    println!("    find [{{...}}]           - Find documents (optional query)");
    println!("    count [{{...}}]          - Count documents (optional query)");
    println!("    update {{query}} {{update}} - Update documents");
    println!("    delete {{query}}         - Delete documents");
    println!();
    println!("{}", "  Index Commands:".bright_yellow());
    println!("    indexes                 - List indexes");
    println!("    create-index <field>    - Create index");
    println!("    create-index <field> --unique - Create unique index");
    println!();
    println!("{}", "  Data Commands:".bright_yellow());
    println!("    export <file>           - Export collection to JSON file");
    println!("    import <file>           - Import collection from JSON file");
    println!();
    println!("{}", "  General:".bright_yellow());
    println!("    help                    - Show this help message");
    println!("    exit                    - Exit the shell");
    println!();
}
