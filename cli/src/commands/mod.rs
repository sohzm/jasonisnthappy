pub mod db;
pub mod collection;
pub mod document;
pub mod query;
pub mod index;
pub mod schema;
pub mod metrics;

use anyhow::Result;
use jasonisnthappy::Database;
use crate::formatter::OutputFormat;

#[cfg(feature = "web-ui")]
use jasonisnthappy::WebServer;

pub struct CommandContext {
    pub db: Database,
    pub format: OutputFormat,
    #[cfg(feature = "web-ui")]
    pub web_server: Option<WebServer>,
}

impl CommandContext {
    pub fn new(db_path: &str, format_str: &str) -> Result<Self> {
        let db = Database::open(db_path)?;
        let format = OutputFormat::from_str(format_str);

        Ok(Self {
            db,
            format,
            #[cfg(feature = "web-ui")]
            web_server: None,
        })
    }

    #[cfg(feature = "web-ui")]
    pub fn start_web_ui(&mut self, address: &str) -> Result<()> {
        use crate::formatter::print_info;

        let web_server = self.db.start_web_ui(address)?;
        print_info(&format!("Web UI started at http://{}", address));
        self.web_server = Some(web_server);
        Ok(())
    }

    #[cfg(not(feature = "web-ui"))]
    pub fn start_web_ui(&mut self, _address: &str) -> Result<()> {
        anyhow::bail!("Web UI feature not enabled. Rebuild with --features web-ui")
    }
}
