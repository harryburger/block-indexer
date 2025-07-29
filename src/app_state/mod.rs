use crate::chain_client::ChainClient;
use crate::config::AppConfig;
use crate::database::{ChainClientRegistry, DatabaseClient};
use crate::source::MongoClient;
use once_cell::sync::OnceCell;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseClient,
    pub source_db: Option<MongoClient>,
    pub chain_clients: ChainClientRegistry,
    pub config: AppConfig,
}

static GLOBAL_APP_STATE: OnceCell<Arc<RwLock<AppState>>> = OnceCell::new();

impl AppState {
    pub fn new(
        db: DatabaseClient,
        source_db: Option<MongoClient>,
        chain_clients: ChainClientRegistry,
        config: AppConfig,
    ) -> Self {
        Self {
            db,
            source_db,
            chain_clients,
            config,
        }
    }

    /// Get database client
    pub fn db(&self) -> &DatabaseClient {
        &self.db
    }

    /// Get source database client
    pub fn source_db(&self) -> Option<&MongoClient> {
        self.source_db.as_ref()
    }

    /// Get chain clients registry
    pub fn chain_clients(&self) -> &ChainClientRegistry {
        &self.chain_clients
    }

    pub fn get_chain_client(&self, chain_name: &str) -> Option<&Arc<Mutex<ChainClient>>> {
        self.chain_clients.get(chain_name)
    }

    /// Get chain config by name
    pub fn get_chain_config(&self, chain_name: &str) -> Option<&crate::config::ChainConfig> {
        self.config.chains.get(chain_name)
    }

    /// Get all chain names
    pub fn get_chain_names(&self) -> Vec<&String> {
        self.config.chains.keys().collect()
    }

    /// Get enabled chain names only
    pub fn get_enabled_chain_names(&self) -> Vec<&String> {
        self.config
            .chains
            .iter()
            .filter(|(_, config)| config.enabled)
            .map(|(name, _)| name)
            .collect()
    }

    /// Get app config
    pub fn config(&self) -> &AppConfig {
        &self.config
    }
}

/// Initialize the global application state
/// This should be called once during application startup
pub fn init_global_state(app_state: AppState) -> Result<(), Box<dyn std::error::Error>> {
    GLOBAL_APP_STATE
        .set(Arc::new(RwLock::new(app_state)))
        .map_err(|_| "Global state already initialized")?;
    Ok(())
}

/// Get a reference to the global application state
/// Returns None if the global state hasn't been initialized
pub fn get_global_state() -> Option<Arc<RwLock<AppState>>> {
    GLOBAL_APP_STATE.get().cloned()
}

/// Convenience function to access the database client from global state
pub fn get_db_client() -> Option<DatabaseClient> {
    get_global_state()?
        .read()
        .ok()
        .map(|state| state.db.clone())
}

/// Convenience function to access the source database client from global state
pub fn get_source_db_client() -> Option<MongoClient> {
    get_global_state()?.read().ok()?.source_db.clone()
}

/// Convenience function to access the chain clients registry from global state
pub fn get_chain_clients() -> Option<ChainClientRegistry> {
    get_global_state()?
        .read()
        .ok()
        .map(|state| state.chain_clients.clone())
}

/// Get chain client by name from global state
pub fn get_chain_client(chain_name: &str) -> Option<Arc<Mutex<ChainClient>>> {
    get_global_state()?
        .read()
        .ok()?
        .get_chain_client(chain_name)
        .cloned()
}

/// Get chain config by name from global state
pub fn get_chain_config(chain_name: &str) -> Option<crate::config::ChainConfig> {
    get_global_state()?
        .read()
        .ok()?
        .get_chain_config(chain_name)
        .cloned()
}

/// Get app config from global state
pub fn get_app_config() -> Option<AppConfig> {
    get_global_state()?
        .read()
        .ok()
        .map(|state| state.config.clone())
}

/// Execute a closure with read access to the global state
pub fn with_global_state<T, F>(f: F) -> Option<T>
where
    F: FnOnce(&AppState) -> T,
{
    let state = get_global_state()?;
    let guard = state.read().ok()?;
    Some(f(&*guard))
}

/// Execute a closure with write access to the global state
pub fn with_global_state_mut<T, F>(f: F) -> Option<T>
where
    F: FnOnce(&mut AppState) -> T,
{
    let state = get_global_state()?;
    let mut guard = state.write().ok()?;
    Some(f(&mut *guard))
}
