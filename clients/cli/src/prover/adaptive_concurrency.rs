//! Adaptive concurrency scaling based on real-time system metrics

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use once_cell::sync::Lazy;

/// Real-time system metrics for adaptive scaling
#[derive(Debug, Clone)]
pub struct SystemMetrics {
    pub cpu_usage_percent: f64,
    pub memory_usage_percent: f64,
    pub proof_completion_rate: f64, // proofs per second
    pub last_updated: Instant,
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_usage_percent: 0.0,
            proof_completion_rate: 0.0,
            last_updated: Instant::now(),
        }
    }
}

/// Adaptive concurrency manager that scales worker count based on system performance
pub struct AdaptiveConcurrencyManager {
    base_workers: usize,
    max_workers: usize,
    min_workers: usize,
    metrics: Arc<Mutex<SystemMetrics>>,
    last_adjustment: Instant,
    adjustment_cooldown: Duration,
    performance_history: Arc<Mutex<Vec<f64>>>,
}

impl AdaptiveConcurrencyManager {
    pub fn new(base_workers: usize, max_workers: usize) -> Self {
        let min_workers = (base_workers / 4).max(1); // Don't go below 1 worker

        Self {
            base_workers,
            max_workers,
            min_workers,
            metrics: Arc::new(Mutex::new(SystemMetrics::default())),
            last_adjustment: Instant::now(),
            adjustment_cooldown: Duration::from_secs(30), // Adjust every 30 seconds
            performance_history: Arc::new(Mutex::new(Vec::with_capacity(10))),
        }
    }

    /// Get current optimal worker count based on system metrics
    pub fn get_optimal_workers(&self) -> usize {
        let metrics = self.metrics.lock().unwrap();
        let current_time = Instant::now();

        // Only adjust if cooldown has passed
        if current_time.duration_since(self.last_adjustment) < self.adjustment_cooldown {
            return self.base_workers;
        }

        // Calculate optimal workers based on system state
        let mut optimal = self.base_workers;

        // Scale based on memory usage
        if metrics.memory_usage_percent < 70.0 {
            // Memory is available, can scale up
            optimal = (optimal as f64 * 1.5) as usize;
        } else if metrics.memory_usage_percent > 85.0 {
            // Memory pressure, scale down
            optimal = (optimal as f64 * 0.7) as usize;
        }

        // Scale based on CPU usage
        if metrics.cpu_usage_percent < 60.0 {
            // CPU has capacity, can scale up
            optimal = (optimal as f64 * 1.2) as usize;
        } else if metrics.cpu_usage_percent > 90.0 {
            // CPU saturated, scale down
            optimal = (optimal as f64 * 0.8) as usize;
        }

        // Scale based on proof completion rate
        if metrics.proof_completion_rate > 0.5 {
            // Good performance, can scale up
            optimal = (optimal as f64 * 1.1) as usize;
        } else if metrics.proof_completion_rate < 0.1 {
            // Poor performance, scale down
            optimal = (optimal as f64 * 0.9) as usize;
        }

        // Apply bounds
        optimal = optimal.clamp(self.min_workers, self.max_workers);

        optimal
    }

    /// Update system metrics with new measurements
    pub fn update_metrics(&self, cpu_usage: f64, memory_usage: f64, proof_completion_rate: f64) {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.cpu_usage_percent = cpu_usage;
        metrics.memory_usage_percent = memory_usage;
        metrics.proof_completion_rate = proof_completion_rate;
        metrics.last_updated = Instant::now();

        // Update performance history
        let mut history = self.performance_history.lock().unwrap();
        history.push(proof_completion_rate);
        if history.len() > 10 {
            history.remove(0);
        }
    }

    /// Record a proof completion for performance tracking
    pub fn record_proof_completion(&self, duration: Duration) {
        let rate = 1.0 / duration.as_secs_f64();

        // Update moving average of completion rate
        let mut metrics = self.metrics.lock().unwrap();
        metrics.proof_completion_rate = (metrics.proof_completion_rate * 0.7) + (rate * 0.3);
    }

    /// Start background monitoring task
    pub async fn start_monitoring(&self) {
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await;

                // Collect system metrics
                let cpu_usage = get_cpu_usage().await;
                let memory_usage = get_memory_usage().await;

                // Update metrics with proper scoping
                {
                    let mut m = metrics.lock().unwrap();
                    m.cpu_usage_percent = cpu_usage;
                    m.memory_usage_percent = memory_usage;
                    m.last_updated = Instant::now();
                } // Lock is dropped here

                // Small delay to prevent busy waiting
                sleep(Duration::from_millis(100)).await;
            }
        });
    }

    /// Get current performance trend
    pub fn get_performance_trend(&self) -> f64 {
        let history = self.performance_history.lock().unwrap();
        if history.len() < 2 {
            return 0.0;
        }

        // Calculate simple trend: recent vs older performance
        let recent_avg = history.iter().rev().take(3).sum::<f64>() / 3.0;
        let older_avg = history.iter().take(3).sum::<f64>() / 3.0;

        if older_avg == 0.0 {
            0.0
        } else {
            (recent_avg - older_avg) / older_avg
        }
    }
}

/// Global adaptive concurrency manager
pub static ADAPTIVE_MANAGER: Lazy<AdaptiveConcurrencyManager> = Lazy::new(|| {
    let total_memory_gb = crate::system::total_memory_gb();
    let cores = crate::system::num_cores();

    // Calculate base workers based on system resources
    let base_workers = if total_memory_gb >= 32.0 {
        cores * 4
    } else if total_memory_gb >= 16.0 {
        cores * 3
    } else if total_memory_gb >= 8.0 {
        cores * 2
    } else {
        cores.max(1)
    };

    let max_workers = base_workers * 2; // Allow 2x scaling

    AdaptiveConcurrencyManager::new(base_workers, max_workers)
});

/// Get current CPU usage (simplified implementation)
async fn get_cpu_usage() -> f64 {
    // This is a simplified implementation
    // In a real scenario, you'd use platform-specific APIs
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(stat) = fs::read_to_string("/proc/stat") {
            // Parse /proc/stat for CPU usage
            // This is a simplified version
            return 50.0; // Placeholder
        }
    }

    // Fallback: estimate based on system load
    60.0 // Reasonable default
}

/// Get current memory usage (simplified implementation)
async fn get_memory_usage() -> f64 {
    let total_memory_gb = crate::system::total_memory_gb();

    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            // Parse /proc/meminfo for memory usage
            // This is a simplified version
            return (total_memory_gb * 0.6 * 100.0) / total_memory_gb; // 60% usage placeholder
        }
    }

    // Fallback: estimate based on available memory
    (total_memory_gb * 0.6 * 100.0) / total_memory_gb // 60% usage
}

/// Initialize adaptive concurrency system
pub async fn initialize_adaptive_concurrency() {
    ADAPTIVE_MANAGER.start_monitoring().await;
}

/// Get optimal worker count for current system state
pub fn get_optimal_worker_count() -> usize {
    ADAPTIVE_MANAGER.get_optimal_workers()
}

/// Record proof completion for adaptive tuning
pub fn record_proof_completion(duration: Duration) {
    ADAPTIVE_MANAGER.record_proof_completion(duration);
}

/// Update system metrics manually
pub fn update_system_metrics(cpu_usage: f64, memory_usage: f64, completion_rate: f64) {
    ADAPTIVE_MANAGER.update_metrics(cpu_usage, memory_usage, completion_rate);
}