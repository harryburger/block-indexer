use anyhow::Result;
use block_injector::{
    api::{create_router, AppState},
    config::AppConfig,
    database::{DatabaseClient, ChainClientRegistry},
    poller::EventListener,
    source::MongoClient,
    utils::setup_logging,
};
use std::sync::Arc;
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging first
    setup_logging()?;
    info!("🚀 Starting Block Injector...");

    // Load and validate configuration
    let config = load_configuration().await?;

    // Initialize all services
    let services = initialize_services(&config).await?;

    // Start all services concurrently
    let handles = start_services(services).await?;

    // Wait for graceful shutdown
    graceful_shutdown(handles).await;

    info!("👋 Block Injector shutdown completed");
    Ok(())
}

/// Load and validate application configuration
async fn load_configuration() -> Result<AppConfig> {
    info!("📋 Loading configuration...");

    let config = AppConfig::from_file("config")?;

    // Validate critical configuration
    if config.chains.is_empty() {
        return Err(anyhow::anyhow!("No chains configured"));
    }

    let enabled_chains: Vec<_> = config
        .chains
        .iter()
        .filter(|(_, chain_config)| chain_config.enabled)
        .map(|(name, _)| name)
        .collect();

    if enabled_chains.is_empty() {
        return Err(anyhow::anyhow!("No enabled chains found in configuration"));
    }

    info!(
        "✅ Configuration loaded with {} enabled chains: {:?}",
        enabled_chains.len(),
        enabled_chains
    );

    Ok(config)
}

/// Initialize all required services
async fn initialize_services(config: &AppConfig) -> Result<Services> {
    info!("🔧 Initializing services...");

    let (db, source_db) = connect_databases(config).await?;
    let event_listener =
        create_event_listener(config.clone(), db.clone(), source_db.clone()).await?;
    
    // Get chain clients from event listener for app state and database
    let chain_clients = event_listener.get_chain_clients();
    
    // Update database client with chain clients for handler access
    let db_with_clients = (*db).clone().with_chain_clients(chain_clients.clone());
    let db_arc = Arc::new(db_with_clients);
    
    let app_state = create_app_state(db_arc.clone(), source_db, chain_clients);

    Ok(Services {
        event_listener,
        app_state,
    })
}

/// Start all services and return their handles
async fn start_services(services: Services) -> Result<ServiceHandles> {
    info!("🚀 Starting all services...");

    let event_listener_handle = start_event_listener(services.event_listener).await?;
    let server_handle = start_api_server(services.app_state).await?;

    info!("✅ All services started successfully");

    Ok(ServiceHandles {
        server_handle,
        event_listener_handle,
    })
}

/// Connect to MongoDB databases with improved error handling
async fn connect_databases(
    config: &AppConfig,
) -> Result<(Arc<DatabaseClient>, Option<MongoClient>)> {
    info!("🔌 Connecting to databases...");

    // Main database connection with defaults
    let mongodb_url = config
        .app
        .mongodb_url
        .as_deref()
        .unwrap_or("mongodb://localhost:27017");

    let mongodb_database = config
        .app
        .mongodb_database
        .as_deref()
        .unwrap_or("block_injector");

    let db = Arc::new(
        DatabaseClient::new(mongodb_url, mongodb_database)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to connect to main database '{}': {}",
                    mongodb_database,
                    e
                )
            })?,
    );

    info!(
        "✅ Connected to main database: {} at {}",
        mongodb_database, mongodb_url
    );

    // Optional source database connection
    let source_db = match (
        &config.app.source_mongodb_url,
        &config.app.source_mongodb_database,
    ) {
        (Some(source_url), Some(source_db_name)) => {
            match MongoClient::new(source_url, source_db_name).await {
                Ok(client) => {
                    info!(
                        "✅ Connected to source database: {} at {}",
                        source_db_name, source_url
                    );
                    Some(client)
                }
                Err(e) => {
                    warn!(
                        "⚠️ Failed to connect to source database '{}': {}",
                        source_db_name, e
                    );
                    None
                }
            }
        }
        _ => {
            info!("📋 No source database configured, skipping");
            None
        }
    };

    Ok((db, source_db))
}

/// Create and initialize event listener
async fn create_event_listener(
    config: AppConfig,
    db: Arc<DatabaseClient>,
    source_db: Option<MongoClient>, // Add this parameter
) -> Result<EventListener> {
    info!("🔍 Creating event listener...");

    let mut event_listener = EventListener::new(config, db, source_db); // Pass source_db

    event_listener
        .initialize()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize event listener: {}", e))?;

    info!("✅ Event listener created and initialized");
    Ok(event_listener)
}

/// Create application state for API server
fn create_app_state(
    db: Arc<DatabaseClient>, 
    source_db: Option<MongoClient>, 
    chain_clients: ChainClientRegistry
) -> AppState {
    AppState {
        db: (*db).clone(),
        source_db,
        chain_clients,
    }
}

/// Start the event listener in a background task
async fn start_event_listener(
    mut event_listener: EventListener,
) -> Result<tokio::task::JoinHandle<()>> {
    info!("🔍 Starting event listener...");

    let handle = tokio::spawn(async move {
        if let Err(e) = event_listener.start().await {
            error!("❌ Event listener error: {}", e);
        } else {
            info!("🔍 Event listener completed gracefully");
        }
    });

    Ok(handle)
}

/// Start the API server with proper configuration
async fn start_api_server(app_state: AppState) -> Result<tokio::task::JoinHandle<()>> {
    info!("📡 Starting API server...");

    // Create HTTP router with comprehensive middleware
    let app = create_router(app_state).layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http())
            .layer(CorsLayer::permissive()),
    );

    // Bind to configurable address
    let bind_address = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to {}: {}", bind_address, e))?;

    info!("✅ API server listening on http://{}", bind_address);

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
        {
            error!("❌ API server error: {}", e);
        } else {
            info!("📡 API server completed gracefully");
        }
    });

    Ok(handle)
}

/// Handle graceful shutdown with improved logging and error handling
async fn graceful_shutdown(handles: ServiceHandles) {
    info!("🎯 All services running. Press Ctrl+C to shutdown gracefully...");

    let ServiceHandles {
        mut server_handle,
        mut event_listener_handle,
    } = handles;

    // Wait for any service to complete or shutdown signal
    tokio::select! {
        result = &mut server_handle => {
            match result {
                Ok(_) => info!("📡 API server shut down gracefully"),
                Err(e) => error!("❌ API server task panicked: {}", e),
            }
        }
        result = &mut event_listener_handle => {
            match result {
                Ok(_) => info!("🔍 Event listener shut down gracefully"),
                Err(e) => error!("❌ Event listener task panicked: {}", e),
            }
        }
    }

    // Abort any remaining tasks
    server_handle.abort();
    event_listener_handle.abort();

    info!("✅ Graceful shutdown completed");
}

/// Enhanced shutdown signal handling
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("🛑 Received Ctrl+C, initiating graceful shutdown...");
        }
        _ = terminate => {
            info!("🛑 Received SIGTERM, initiating graceful shutdown...");
        }
    }
}

/// Container for all initialized services
struct Services {
    event_listener: EventListener,
    app_state: AppState,
}

/// Container for all service handles
struct ServiceHandles {
    server_handle: tokio::task::JoinHandle<()>,
    event_listener_handle: tokio::task::JoinHandle<()>,
}
