//! Adaptive dynamic batching for optimal resource utilization

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Adaptive batch size calculator that adjusts based on system performance
pub struct AdaptiveBatcher {
    state: Arc<RwLock<BatcherState>>,
    metrics: Arc<BatchMetrics>,
}

#[derive(Debug, Clone)]
struct BatcherState {
    current_batch_size: usize,
    target_proof_time: Duration,  // Target time per proof (e.g., 2 seconds)
    last_adjustment: Instant,
    consecutive_good_batches: usize,
    consecutive_bad_batches: usize,
    min_batch_size: usize,
    max_batch_size: usize,
}

/// Performance metrics for adaptive batching
#[derive(Debug)]
pub struct BatchMetrics {
    pub total_proofs: AtomicUsize,
    pub total_batches: AtomicUsize,
    pub total_time: AtomicU64,  // Store nanoseconds
    pub last_proof_time: AtomicU64,  // Store last proof time in nanoseconds
    pub average_proof_time: AtomicU64,  // Moving average
}

impl BatchMetrics {
    pub fn new() -> Self {
        Self {
            total_proofs: AtomicUsize::new(0),
            total_batches: AtomicUsize::new(0),
            total_time: AtomicU64::new(0),
            last_proof_time: AtomicU64::new(0),
            average_proof_time: AtomicU64::new(0),
        }
    }

    pub fn record_proof_time(&self, time_nanos: u64) {
        self.total_proofs.fetch_add(1, Ordering::Relaxed);
        self.total_time.fetch_add(time_nanos, Ordering::Relaxed);
        self.last_proof_time.store(time_nanos, Ordering::Relaxed);

        // Update moving average
        let total_proofs = self.total_proofs.load(Ordering::Relaxed);
        if total_proofs > 0 {
            let total_time = self.total_time.load(Ordering::Relaxed);
            let avg = total_time / total_proofs as u64;
            self.average_proof_time.store(avg, Ordering::Relaxed);
        }
    }

    pub fn record_batch(&self) {
        self.total_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_average_proof_time(&self) -> Duration {
        let nanos = self.average_proof_time.load(Ordering::Relaxed);
        Duration::from_nanos(nanos)
    }

    pub fn get_last_proof_time(&self) -> Duration {
        let nanos = self.last_proof_time.load(Ordering::Relaxed);
        Duration::from_nanos(nanos)
    }
}

impl AdaptiveBatcher {
    /// Create a new adaptive batcher
    pub fn new() -> Self {
        Self::with_bounds(4, 20)
    }

    /// Create a new adaptive batcher with custom bounds
    pub fn with_bounds(min_batch_size: usize, max_batch_size: usize) -> Self {
        let total_memory_gb = crate::system::total_memory_gb();
        
        // For ultra-low memory, force minimal batching to prevent OOM
        let (adjusted_min, adjusted_max, initial_size) = if total_memory_gb < 1.5 {
            eprintln!("Ultra-low memory detected ({:.1}GB) - disabling batching (batch size = 1)", total_memory_gb);
            (1, 1, 1) // No batching at all for <1.5GB
        } else if total_memory_gb < 2.0 {
            eprintln!("Low memory detected ({:.1}GB) - minimal batching (max batch size = 2)", total_memory_gb);
            (1, 2, 1) // Minimal batching for 1.5-2GB
        } else {
            (min_batch_size, max_batch_size, std::cmp::max(4, min_batch_size))
        };
        
        let state = BatcherState {
            current_batch_size: initial_size,
            target_proof_time: Duration::from_millis(2000), // Target 2 seconds per proof
            last_adjustment: Instant::now(),
            consecutive_good_batches: 0,
            consecutive_bad_batches: 0,
            min_batch_size: adjusted_min,
            max_batch_size: adjusted_max,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
            metrics: Arc::new(BatchMetrics::new()),
        }
    }

    /// Get the current optimal batch size
    pub async fn get_optimal_batch_size(&self) -> usize {
        let state = self.state.read().await;
        state.current_batch_size
    }

    /// Get the current batch size with memory-based adjustment
    pub async fn get_memory_adjusted_batch_size(&self, input_count: usize) -> usize {
        // For single tasks, don't batch at all
        if input_count == 1 {
            return 1;
        }

        let base_batch_size = self.get_optimal_batch_size().await;

        // Additional memory-based adjustment
        let total_memory_gb = crate::system::total_memory_gb();
        let memory_multiplier = match total_memory_gb {
            mem if mem >= 32.0 => 1.5,
            mem if mem >= 16.0 => 1.2,
            mem if mem >= 8.0 => 1.0,
            mem if mem >= 4.0 => 0.8,
            mem if mem >= 2.0 => 0.6,
            mem if mem >= 1.5 => 0.4,
            _ => 0.2, // Ultra-low memory - essentially force batch size = 1
        };

        let adjusted_size = (base_batch_size as f64 * memory_multiplier) as usize;

        // Don't let batch size exceed input count, and ensure minimum batch size
        std::cmp::min(std::cmp::max(adjusted_size, 1), input_count)
    }

    /// Record performance metrics for a batch
    pub async fn record_batch_performance(&self,
        batch_size: usize,
        total_batch_time: Duration,
        proofs_generated: usize
    ) {
        // Record batch
        self.metrics.record_batch();

        // Record individual proof times
        if proofs_generated > 0 {
            let avg_proof_time = total_batch_time / proofs_generated as u32;
            let time_nanos = avg_proof_time.as_nanos() as u64;

            for _ in 0..proofs_generated {
                self.metrics.record_proof_time(time_nanos);
            }
        }

        // Adjust batch size based on performance
        self.adjust_batch_size(batch_size, total_batch_time, proofs_generated).await;
    }

    /// Adjust batch size based on recent performance
    async fn adjust_batch_size(&self,
        batch_size: usize,
        total_batch_time: Duration,
        proofs_generated: usize
    ) {
        let mut state = self.state.write().await;

        // Don't adjust too frequently
        if state.last_adjustment.elapsed() < Duration::from_secs(30) {
            return;
        }

        if proofs_generated == 0 {
            // Batch failed completely, reduce size
            state.current_batch_size = (state.current_batch_size / 2).max(state.min_batch_size);
            state.consecutive_bad_batches += 1;
            state.consecutive_good_batches = 0;
            state.last_adjustment = Instant::now();
            return;
        }

        let avg_proof_time = total_batch_time / proofs_generated as u32;
        let performance_ratio = avg_proof_time.as_millis() as f64 / state.target_proof_time.as_millis() as f64;

        // Adjust based on performance
        let new_batch_size = if performance_ratio > 1.5 {
            // Too slow, reduce batch size
            (batch_size as f64 * 0.8) as usize
        } else if performance_ratio > 1.2 {
            // Slightly slow, small reduction
            (batch_size as f64 * 0.9) as usize
        } else if performance_ratio < 0.7 && total_memory_gb() >= 8.0 {
            // Very fast and enough memory, increase batch size
            (batch_size as f64 * 1.3) as usize
        } else if performance_ratio < 0.9 && total_memory_gb() >= 4.0 {
            // Fast and moderate memory, small increase
            (batch_size as f64 * 1.1) as usize
        } else {
            // Good performance, no change
            batch_size
        };

        // Apply bounds
        let new_batch_size = new_batch_size
            .max(state.min_batch_size)
            .min(state.max_batch_size);

        // Only update if there's a meaningful change
        if (new_batch_size as i32 - state.current_batch_size as i32).abs() >= 2 {
            state.current_batch_size = new_batch_size;
            state.last_adjustment = Instant::now();

                    }

        // Track consecutive good/bad batches
        if performance_ratio <= 1.2 {
            state.consecutive_good_batches += 1;
            state.consecutive_bad_batches = 0;
        } else {
            state.consecutive_bad_batches += 1;
            state.consecutive_good_batches = 0;
        }

        // Aggressive adjustment for consecutive issues
        if state.consecutive_bad_batches >= 3 {
            state.current_batch_size = (state.current_batch_size / 2).max(state.min_batch_size);
            state.consecutive_bad_batches = 0;
        } else if state.consecutive_good_batches >= 5 {
            state.current_batch_size = (state.current_batch_size * 3 / 2).min(state.max_batch_size);
            state.consecutive_good_batches = 0;
        }
    }

    /// Get current performance metrics
    pub async fn get_metrics(&self) -> BatchMetricsSnapshot {
        let state = self.state.read().await;
        BatchMetricsSnapshot {
            current_batch_size: state.current_batch_size,
            target_proof_time: state.target_proof_time,
            consecutive_good_batches: state.consecutive_good_batches,
            consecutive_bad_batches: state.consecutive_bad_batches,
            total_proofs: self.metrics.total_proofs.load(Ordering::Relaxed),
            total_batches: self.metrics.total_batches.load(Ordering::Relaxed),
            average_proof_time: self.metrics.get_average_proof_time(),
            last_proof_time: self.metrics.get_last_proof_time(),
        }
    }
}

/// Snapshot of current batcher state and metrics
#[derive(Debug, Clone)]
pub struct BatchMetricsSnapshot {
    pub current_batch_size: usize,
    pub target_proof_time: Duration,
    pub consecutive_good_batches: usize,
    pub consecutive_bad_batches: usize,
    pub total_proofs: usize,
    pub total_batches: usize,
    pub average_proof_time: Duration,
    pub last_proof_time: Duration,
}

/// Get total system memory in GB
fn total_memory_gb() -> f64 {
    crate::system::total_memory_gb()
}

/// Global adaptive batcher instance
pub static GLOBAL_ADAPTIVE_BATCHER: std::sync::LazyLock<AdaptiveBatcher> = std::sync::LazyLock::new(|| {
    AdaptiveBatcher::with_bounds(4, 20)
});

/// Get the global adaptive batcher
pub fn get_global_batcher() -> &'static AdaptiveBatcher {
    &GLOBAL_ADAPTIVE_BATCHER
}

/// Reset the global adaptive batcher state (for memory cleanup)
pub fn reset_global_batcher() {
    // Note: This is a workaround for the fact that LazyLock doesn't support resetting
    // In a production environment, we'd implement a proper reset mechanism
    println!("[WARNING] Cannot reset global batcher - LazyLock doesn't support reset. Consider avoiding global state for low-memory systems.");
}

/// Check if we should avoid global state for memory-constrained systems
pub fn should_use_global_batcher() -> bool {
    crate::system::total_memory_gb() > 2.0
}