//! HTTP/2 connection pool for multiplexed network requests

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use once_cell::sync::Lazy;

/// Configuration for connection pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections_per_host: usize,
    pub connection_idle_timeout: Duration,
    pub max_idle_connections: usize,
    pub enable_http2: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_host: 10,
            connection_idle_timeout: Duration::from_secs(300), // 5 minutes
            max_idle_connections: 50,
            enable_http2: true,
        }
    }
}

/// Pooled connection with metadata
struct PooledConnection {
    client: reqwest::Client,
    last_used: Instant,
    in_use: bool,
    request_count: u64,
}

impl PooledConnection {
    fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            last_used: Instant::now(),
            in_use: false,
            request_count: 0,
        }
    }

    fn is_expired(&self, timeout: Duration) -> bool {
        self.last_used.elapsed() > timeout
    }

    fn mark_used(&mut self) {
        self.last_used = Instant::now();
        self.request_count += 1;
    }
}

/// HTTP/2 connection pool for efficient network multiplexing
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<String, Vec<PooledConnection>>>>,
    config: PoolConfig,
    stats: Arc<Mutex<PoolStats>>,
}

#[derive(Debug, Default)]
pub struct PoolStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub idle_connections: usize,
    pub requests_served: u64,
    pub connections_created: u64,
    pub connections_reused: u64,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(Mutex::new(PoolStats::default())),
        }
    }

    /// Get a client for the given host, creating one if necessary
    pub async fn get_client(&self, base_url: &str) -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
        let host = self.extract_host(base_url)?;
        let mut connections = self.connections.write().await;

        // Get or create connection list for this host
        let host_connections = connections.entry(host.clone()).or_insert_with(Vec::new);

        // Try to find an available connection
        for conn in host_connections.iter_mut() {
            if !conn.in_use && !conn.is_expired(self.config.connection_idle_timeout) {
                conn.in_use = true;
                conn.mark_used();

                // Update stats
                let mut stats = self.stats.lock().unwrap();
                stats.active_connections += 1;
                stats.idle_connections = stats.idle_connections.saturating_sub(1);
                stats.requests_served += 1;
                stats.connections_reused += 1;

                return Ok(conn.client.clone());
            }
        }

        // Clean up expired connections
        host_connections.retain(|conn| !conn.is_expired(self.config.connection_idle_timeout));

        // Check if we can create a new connection
        if host_connections.len() >= self.config.max_connections_per_host {
            // Pool is full, wait for a connection to become available or reuse least recently used
            if let Some(conn) = host_connections.iter_mut()
                .filter(|c| !c.in_use)
                .min_by_key(|c| c.last_used) {
                conn.in_use = true;
                conn.mark_used();

                let mut stats = self.stats.lock().unwrap();
                stats.requests_served += 1;
                stats.connections_reused += 1;

                return Ok(conn.client.clone());
            }
        }

        // Create new connection
        let client = self.create_client()?;
        let mut new_conn = PooledConnection::new(client.clone());
        new_conn.in_use = true;
        new_conn.mark_used();

        host_connections.push(new_conn);

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.total_connections += 1;
        stats.active_connections += 1;
        stats.requests_served += 1;
        stats.connections_created += 1;

        Ok(client)
    }

    /// Return a connection to the pool
    pub async fn return_client(&self, base_url: &str) {
        let host = match self.extract_host(base_url) {
            Ok(h) => h,
            Err(_) => return,
        };

        let mut connections = self.connections.write().await;
        if let Some(host_connections) = connections.get_mut(&host) {
            for conn in host_connections.iter_mut() {
                if conn.in_use {
                    conn.in_use = false;
                    conn.last_used = Instant::now();

                    // Update stats
                    let mut stats = self.stats.lock().unwrap();
                    stats.active_connections = stats.active_connections.saturating_sub(1);
                    stats.idle_connections += 1;
                    break;
                }
            }
        }
    }

    /// Create a new HTTP client with HTTP/2 support
    fn create_client(&self) -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .pool_idle_timeout(self.config.connection_idle_timeout)
            .pool_max_idle_per_host(self.config.max_idle_connections);

        if self.config.enable_http2 {
            // HTTP/2 is enabled by default in reqwest when available
            // Additional HTTP/2 specific optimizations can be added here
        }

        // Enable TCP keepalive
        builder = builder.tcp_keepalive(Duration::from_secs(60));

        // Enable connection reuse
        builder = builder.connection_verbose(true);

        // Set user agent
        builder = builder.user_agent("nexus-cli/1.0");

        builder.build().map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Extract host from URL
    fn extract_host(&self, url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let parsed = url::Url::parse(url)?;
        Ok(format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or("localhost")))
    }

    /// Get pool statistics
    pub fn get_stats(&self) -> PoolStats {
        let stats = self.stats.lock().unwrap();
        PoolStats {
            total_connections: stats.total_connections,
            active_connections: stats.active_connections,
            idle_connections: stats.idle_connections,
            requests_served: stats.requests_served,
            connections_created: stats.connections_created,
            connections_reused: stats.connections_reused,
        }
    }

    /// Clean up expired connections
    pub async fn cleanup_expired(&self) {
        let mut connections = self.connections.write().await;
        let mut removed_count = 0;

        for (_, host_connections) in connections.iter_mut() {
            let initial_len = host_connections.len();
            host_connections.retain(|conn| !conn.is_expired(self.config.connection_idle_timeout));
            removed_count += initial_len - host_connections.len();
        }

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.total_connections = stats.total_connections.saturating_sub(removed_count);
        stats.idle_connections = stats.idle_connections.saturating_sub(removed_count);
    }

    /// Start background cleanup task
    pub async fn start_cleanup_task(&self) {
        let connections = Arc::clone(&self.connections);
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Cleanup every minute

            loop {
                interval.tick().await;

                let mut conns = connections.write().await;
                let mut total_removed = 0;

                for (_, host_connections) in conns.iter_mut() {
                    let initial_len = host_connections.len();
                    host_connections.retain(|conn| !conn.is_expired(config.connection_idle_timeout));
                    total_removed += initial_len - host_connections.len();
                }

                if total_removed > 0 {
                    eprintln!("Cleaned up {} expired connections", total_removed);
                }
            }
        });
    }
}

/// Global connection pool instance
pub static GLOBAL_CONNECTION_POOL: Lazy<ConnectionPool> = Lazy::new(|| {
    let config = PoolConfig::default();
    ConnectionPool::new(config)
});

/// Initialize the global connection pool
pub async fn initialize_connection_pool() {
    GLOBAL_CONNECTION_POOL.start_cleanup_task().await;
}

/// Get a client from the global pool
pub async fn get_client(base_url: &str) -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
    GLOBAL_CONNECTION_POOL.get_client(base_url).await
}

/// Return a client to the global pool
pub async fn return_client(base_url: &str) {
    GLOBAL_CONNECTION_POOL.return_client(base_url).await
}

/// Get global pool statistics
pub fn get_pool_stats() -> PoolStats {
    GLOBAL_CONNECTION_POOL.get_stats()
}