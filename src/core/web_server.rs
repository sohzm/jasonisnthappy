#[cfg(feature = "web-ui")]
use crate::core::database::Database;
#[cfg(feature = "web-ui")]
use std::sync::Arc;
#[cfg(feature = "web-ui")]
use std::thread::{self, JoinHandle};
#[cfg(feature = "web-ui")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "web-ui")]
pub struct WebServer {
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

#[cfg(feature = "web-ui")]
impl WebServer {
    pub fn start(db: Arc<Database>, addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let server = tiny_http::Server::http(addr)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn std::error::Error>)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let handle = thread::spawn(move || {
            for mut request in server.incoming_requests() {
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }

                let path = request.url().to_string();
                let method = request.method().as_str();

                let response = match (method, path.as_str()) {
                    ("GET", "/") => serve_dashboard(),
                    ("GET", "/metrics") => serve_metrics(&db),
                    ("GET", "/health") => serve_health(),
                    ("GET", "/api/collections") => serve_collections_list(&db),
                    ("POST", "/api/collections") => serve_create_collection(&db, &mut request),
                    ("GET", path) if path.starts_with("/api/collections/") => {
                        // Path like /api/collections/users or /api/collections/users/doc123
                        let rest = path.strip_prefix("/api/collections/").unwrap_or("");
                        let parts: Vec<&str> = rest.split('/').collect();

                        if parts.len() == 1 && !parts[0].is_empty() {
                            // GET /api/collections/users - list all documents
                            serve_collection_documents(&db, parts[0])
                        } else {
                            serve_404()
                        }
                    }
                    ("POST", path) if path.starts_with("/api/collections/") => {
                        let rest = path.strip_prefix("/api/collections/").unwrap_or("");
                        let parts: Vec<&str> = rest.split('/').collect();

                        if parts.len() == 1 && !parts[0].is_empty() {
                            // POST /api/collections/users - create document
                            serve_create_document(&db, parts[0], &mut request)
                        } else {
                            serve_404()
                        }
                    }
                    ("PUT", path) if path.starts_with("/api/collections/") => {
                        let rest = path.strip_prefix("/api/collections/").unwrap_or("");
                        let parts: Vec<&str> = rest.split('/').collect();

                        if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                            // PUT /api/collections/users/doc123 - update document
                            serve_update_document(&db, parts[0], parts[1], &mut request)
                        } else {
                            serve_404()
                        }
                    }
                    ("PATCH", path) if path.starts_with("/api/collections/") => {
                        let rest = path.strip_prefix("/api/collections/").unwrap_or("");
                        let parts: Vec<&str> = rest.split('/').collect();

                        if parts.len() == 1 && !parts[0].is_empty() {
                            // PATCH /api/collections/users - rename collection
                            serve_rename_collection(&db, parts[0], &mut request)
                        } else {
                            serve_404()
                        }
                    }
                    ("DELETE", path) if path.starts_with("/api/collections/") => {
                        let rest = path.strip_prefix("/api/collections/").unwrap_or("");
                        let parts: Vec<&str> = rest.split('/').collect();

                        if parts.len() == 1 && !parts[0].is_empty() {
                            // DELETE /api/collections/users - delete collection
                            serve_drop_collection(&db, parts[0])
                        } else if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                            // DELETE /api/collections/users/doc123 - delete document
                            serve_delete_document(&db, parts[0], parts[1])
                        } else {
                            serve_404()
                        }
                    }
                    _ => serve_404(),
                };

                let _ = request.respond(response);
            }
        });

        Ok(WebServer {
            handle: Some(handle),
            shutdown,
        })
    }

    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(feature = "web-ui")]
impl Drop for WebServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(feature = "web-ui")]
fn serve_dashboard() -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let html = include_str!("web_ui.html");
    tiny_http::Response::from_string(html)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..])
                .unwrap(),
        )
}

#[cfg(feature = "web-ui")]
fn serve_metrics(db: &Arc<Database>) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let metrics = db.metrics();
    let json = serde_json::to_string_pretty(&metrics).unwrap_or_else(|_| "{}".to_string());

    tiny_http::Response::from_string(json)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                .unwrap(),
        )
}

#[cfg(feature = "web-ui")]
fn serve_health() -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let json = r#"{"status":"ok"}"#;
    tiny_http::Response::from_string(json)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                .unwrap(),
        )
}

#[cfg(feature = "web-ui")]
fn serve_collections_list(db: &Arc<Database>) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let metadata = db.get_metadata();
    let mut collections: Vec<String> = metadata.collections.keys().cloned().collect();
    collections.sort(); // Sort alphabetically
    let json = serde_json::to_string_pretty(&collections).unwrap_or_else(|_| "[]".to_string());

    tiny_http::Response::from_string(json)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                .unwrap(),
        )
}

#[cfg(feature = "web-ui")]
fn serve_collection_documents(
    db: &Arc<Database>,
    collection_name: &str,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    match db.begin() {
        Ok(mut tx) => {
            match tx.collection(collection_name) {
                Ok(coll) => {
                    match coll.find_all() {
                        Ok(docs) => {
                            let json = serde_json::to_string_pretty(&docs)
                                .unwrap_or_else(|_| "[]".to_string());
                            tiny_http::Response::from_string(json)
                                .with_header(
                                    tiny_http::Header::from_bytes(
                                        &b"Content-Type"[..],
                                        &b"application/json"[..],
                                    )
                                    .unwrap(),
                                )
                        }
                        Err(_) => serve_500("Failed to read documents"),
                    }
                }
                Err(_) => serve_404(),
            }
        }
        Err(_) => serve_500("Failed to begin transaction"),
    }
}

#[cfg(feature = "web-ui")]
fn serve_create_document(
    db: &Arc<Database>,
    collection_name: &str,
    request: &mut tiny_http::Request,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // Read request body
    let mut body = String::new();
    if let Err(_) = request.as_reader().read_to_string(&mut body) {
        return serve_500("Failed to read request body");
    }

    // Parse JSON
    let json_value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return serve_500(&format!("Invalid JSON: {}", e)),
    };

    // Create document
    match db.begin() {
        Ok(mut tx) => {
            match tx.collection(collection_name) {
                Ok(mut coll) => {
                    match coll.insert(json_value) {
                        Ok(doc_id) => {
                            let response_json = serde_json::json!({ "id": doc_id });
                            let json = serde_json::to_string(&response_json)
                                .unwrap_or_else(|_| "{}".to_string());

                            // Commit transaction
                            if let Err(e) = tx.commit() {
                                return serve_500(&format!("Failed to commit: {}", e));
                            }

                            tiny_http::Response::from_string(json)
                                .with_status_code(201)
                                .with_header(
                                    tiny_http::Header::from_bytes(
                                        &b"Content-Type"[..],
                                        &b"application/json"[..],
                                    )
                                    .unwrap(),
                                )
                        }
                        Err(e) => serve_500(&format!("Failed to insert document: {}", e)),
                    }
                }
                Err(e) => serve_500(&format!("Failed to access collection: {}", e)),
            }
        }
        Err(e) => serve_500(&format!("Failed to begin transaction: {}", e)),
    }
}

#[cfg(feature = "web-ui")]
fn serve_update_document(
    db: &Arc<Database>,
    collection_name: &str,
    doc_id: &str,
    request: &mut tiny_http::Request,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // Read request body
    let mut body = String::new();
    if let Err(_) = request.as_reader().read_to_string(&mut body) {
        return serve_500("Failed to read request body");
    }

    // Parse JSON
    let json_value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return serve_500(&format!("Invalid JSON: {}", e)),
    };

    // Update document
    match db.begin() {
        Ok(mut tx) => {
            match tx.collection(collection_name) {
                Ok(mut coll) => {
                    match coll.update_by_id(doc_id, json_value) {
                        Ok(_) => {
                            // Commit transaction
                            if let Err(e) = tx.commit() {
                                return serve_500(&format!("Failed to commit: {}", e));
                            }

                            let response_json = serde_json::json!({ "status": "updated" });
                            let json = serde_json::to_string(&response_json)
                                .unwrap_or_else(|_| "{}".to_string());

                            tiny_http::Response::from_string(json)
                                .with_header(
                                    tiny_http::Header::from_bytes(
                                        &b"Content-Type"[..],
                                        &b"application/json"[..],
                                    )
                                    .unwrap(),
                                )
                        }
                        Err(e) => serve_500(&format!("Failed to update document: {}", e)),
                    }
                }
                Err(e) => serve_500(&format!("Failed to access collection: {}", e)),
            }
        }
        Err(e) => serve_500(&format!("Failed to begin transaction: {}", e)),
    }
}

#[cfg(feature = "web-ui")]
fn serve_delete_document(
    db: &Arc<Database>,
    collection_name: &str,
    doc_id: &str,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // Delete document
    match db.begin() {
        Ok(mut tx) => {
            match tx.collection(collection_name) {
                Ok(mut coll) => {
                    match coll.delete_by_id(doc_id) {
                        Ok(_) => {
                            // Commit transaction
                            if let Err(e) = tx.commit() {
                                return serve_500(&format!("Failed to commit: {}", e));
                            }

                            let response_json = serde_json::json!({ "status": "deleted" });
                            let json = serde_json::to_string(&response_json)
                                .unwrap_or_else(|_| "{}".to_string());

                            tiny_http::Response::from_string(json)
                                .with_header(
                                    tiny_http::Header::from_bytes(
                                        &b"Content-Type"[..],
                                        &b"application/json"[..],
                                    )
                                    .unwrap(),
                                )
                        }
                        Err(e) => serve_500(&format!("Failed to delete document: {}", e)),
                    }
                }
                Err(e) => serve_500(&format!("Failed to access collection: {}", e)),
            }
        }
        Err(e) => serve_500(&format!("Failed to begin transaction: {}", e)),
    }
}

#[cfg(feature = "web-ui")]
fn serve_create_collection(
    db: &Arc<Database>,
    request: &mut tiny_http::Request,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // Read request body
    let mut body = String::new();
    if let Err(_) = request.as_reader().read_to_string(&mut body) {
        return serve_500("Failed to read request body");
    }

    // Parse JSON to get collection name
    #[derive(serde::Deserialize)]
    struct CreateCollectionRequest {
        name: String,
    }

    let req: CreateCollectionRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => return serve_500(&format!("Invalid JSON: {}", e)),
    };

    // Create collection using transaction API
    match db.begin() {
        Ok(mut tx) => {
            match tx.create_collection(&req.name) {
                Ok(_) => {
                    // Commit to persist the collection
                    if let Err(e) = tx.commit() {
                        return serve_500(&format!("Failed to commit: {}", e));
                    }

                    let response_json = serde_json::json!({ "status": "created", "name": req.name });
                    let json = serde_json::to_string(&response_json)
                        .unwrap_or_else(|_| "{}".to_string());

                    tiny_http::Response::from_string(json)
                        .with_status_code(201)
                        .with_header(
                            tiny_http::Header::from_bytes(
                                &b"Content-Type"[..],
                                &b"application/json"[..],
                            )
                            .unwrap(),
                        )
                }
                Err(e) => serve_500(&format!("Failed to create collection: {}", e)),
            }
        }
        Err(e) => serve_500(&format!("Failed to begin transaction: {}", e)),
    }
}

#[cfg(feature = "web-ui")]
fn serve_rename_collection(
    db: &Arc<Database>,
    old_name: &str,
    request: &mut tiny_http::Request,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // Read request body
    let mut body = String::new();
    if let Err(_) = request.as_reader().read_to_string(&mut body) {
        return serve_500("Failed to read request body");
    }

    // Parse JSON to get new name
    #[derive(serde::Deserialize)]
    struct RenameCollectionRequest {
        new_name: String,
    }

    let req: RenameCollectionRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => return serve_500(&format!("Invalid JSON: {}", e)),
    };

    // Rename collection
    match db.begin() {
        Ok(mut tx) => {
            match tx.rename_collection(old_name, &req.new_name) {
                Ok(_) => {
                    // Commit to persist the rename
                    if let Err(e) = tx.commit() {
                        return serve_500(&format!("Failed to commit: {}", e));
                    }

                    let response_json = serde_json::json!({
                        "status": "renamed",
                        "old_name": old_name,
                        "new_name": req.new_name
                    });
                    let json = serde_json::to_string(&response_json)
                        .unwrap_or_else(|_| "{}".to_string());

                    tiny_http::Response::from_string(json)
                        .with_header(
                            tiny_http::Header::from_bytes(
                                &b"Content-Type"[..],
                                &b"application/json"[..],
                            )
                            .unwrap(),
                        )
                }
                Err(e) => serve_500(&format!("Failed to rename collection: {}", e)),
            }
        }
        Err(e) => serve_500(&format!("Failed to begin transaction: {}", e)),
    }
}

#[cfg(feature = "web-ui")]
fn serve_drop_collection(
    db: &Arc<Database>,
    name: &str,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    // Delete collection
    match db.begin() {
        Ok(mut tx) => {
            match tx.drop_collection(name) {
                Ok(_) => {
                    // Commit to persist the deletion
                    if let Err(e) = tx.commit() {
                        return serve_500(&format!("Failed to commit: {}", e));
                    }

                    let response_json = serde_json::json!({ "status": "deleted", "name": name });
                    let json = serde_json::to_string(&response_json)
                        .unwrap_or_else(|_| "{}".to_string());

                    tiny_http::Response::from_string(json)
                        .with_header(
                            tiny_http::Header::from_bytes(
                                &b"Content-Type"[..],
                                &b"application/json"[..],
                            )
                            .unwrap(),
                        )
                }
                Err(e) => serve_500(&format!("Failed to delete collection: {}", e)),
            }
        }
        Err(e) => serve_500(&format!("Failed to begin transaction: {}", e)),
    }
}

#[cfg(feature = "web-ui")]
fn serve_404() -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string("Not Found")
        .with_status_code(404)
}

#[cfg(feature = "web-ui")]
fn serve_500(msg: &str) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string(msg)
        .with_status_code(500)
}

