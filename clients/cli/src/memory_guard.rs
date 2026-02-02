//! Memory monitoring and protection for low-memory systems

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Memory guard that monitors system memory usage and enforces limits
pub struct MemoryGuard {
    enabled: AtomicBool,
    last_check: std::sync::Mutex<Instant>,
    warning_threshold_gb: f64,
    critical_threshold_gb: f64,
}

impl MemoryGuard {
    /// Create a new memory guard with the specified thresholds
    pub fn new(warning_threshold_gb: f64, critical_threshold_gb: f64) -> Self {
        Self {
            enabled: AtomicBool::new(true),
            last_check: std::sync::Mutex::new(Instant::now()),
            warning_threshold_gb,
            critical_threshold_gb,
        }
    }

    /// Create a memory guard configured for the current system
    pub fn for_current_system() -> Self {
        let total_memory_gb = crate::system::total_memory_gb();
        
        // For low-memory systems, set aggressive thresholds
        let (warning_threshold, critical_threshold) = if total_memory_gb < 2.0 {
            // For <2GB systems, warn at 70% usage, critical at 85%
            (total_memory_gb * 0.7, total_memory_gb * 0.85)
        } else if total_memory_gb < 4.0 {
            // For 2-4GB systems, warn at 75%, critical at 90%
            (total_memory_gb * 0.75, total_memory_gb * 0.90)
        } else {
            // For >4GB systems, warn at 80%, critical at 95%
            (total_memory_gb * 0.80, total_memory_gb * 0.95)
        };

        eprintln!("Memory guard initialized: {:.1}GB total, warning at {:.1}GB, critical at {:.1}GB",
                  total_memory_gb, warning_threshold, critical_threshold);

        Self::new(warning_threshold, critical_threshold)
    }

    /// Check memory usage and return the current status
    pub fn check_memory(&self) -> MemoryStatus {
        if !self.enabled.load(Ordering::Relaxed) {
            return MemoryStatus::Ok;
        }

        let process_memory_gb = crate::system::process_memory_gb();
        
        if process_memory_gb >= self.critical_threshold_gb {
            MemoryStatus::Critical {
                current_gb: process_memory_gb,
                threshold_gb: self.critical_threshold_gb,
            }
        } else if process_memory_gb >= self.warning_threshold_gb {
            MemoryStatus::Warning {
                current_gb: process_memory_gb,
                threshold_gb: self.warning_threshold_gb,
            }
        } else {
            MemoryStatus::Ok
        }
    }

    /// Check memory usage with rate limiting (don't check too frequently)
    pub fn check_memory_throttled(&self, min_interval: Duration) -> Option<MemoryStatus> {
        let mut last_check = self.last_check.lock().unwrap();
        
        if last_check.elapsed() < min_interval {
            return None;
        }

        *last_check = Instant::now();
        drop(last_check);

        Some(self.check_memory())
    }

    /// Disable memory monitoring
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    /// Enable memory monitoring
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }
}

/// Memory usage status
#[derive(Debug, Clone)]
pub enum MemoryStatus {
    /// Memory usage is within acceptable limits
    Ok,
    /// Memory usage is approaching limits (warning threshold exceeded)
    Warning {
        current_gb: f64,
        threshold_gb: f64,
    },
    /// Memory usage is critically high (critical threshold exceeded)
    Critical {
        current_gb: f64,
        threshold_gb: f64,
    },
}

impl MemoryStatus {
    /// Check if memory status is critical
    pub fn is_critical(&self) -> bool {
        matches!(self, MemoryStatus::Critical { .. })
    }

    /// Check if memory status is warning or critical
    pub fn is_concerning(&self) -> bool {
        matches!(self, MemoryStatus::Warning { .. } | MemoryStatus::Critical { .. })
    }

    /// Get a human-readable description of the status
    pub fn description(&self) -> String {
        match self {
            MemoryStatus::Ok => "Memory usage OK".to_string(),
            MemoryStatus::Warning { current_gb, threshold_gb } => {
                format!("Memory warning: {:.1}GB used (threshold: {:.1}GB)", current_gb, threshold_gb)
            }
            MemoryStatus::Critical { current_gb, threshold_gb } => {
                format!("Memory critical: {:.1}GB used (threshold: {:.1}GB)", current_gb, threshold_gb)
            }
        }
    }
}

/// Global memory guard instance
pub static GLOBAL_MEMORY_GUARD: std::sync::LazyLock<MemoryGuard> = std::sync::LazyLock::new(|| {
    MemoryGuard::for_current_system()
});

/// Check global memory status
pub fn check_global_memory() -> MemoryStatus {
    GLOBAL_MEMORY_GUARD.check_memory()
}

/// Check global memory status with throttling
pub fn check_global_memory_throttled(min_interval: Duration) -> Option<MemoryStatus> {
    GLOBAL_MEMORY_GUARD.check_memory_throttled(min_interval)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_guard_creation() {
        let guard = MemoryGuard::new(1.0, 1.5);
        assert_eq!(guard.warning_threshold_gb, 1.0);
        assert_eq!(guard.critical_threshold_gb, 1.5);
    }

    #[test]
    fn test_memory_status() {
        let status = MemoryStatus::Ok;
        assert!(!status.is_critical());
        assert!(!status.is_concerning());

        let warning = MemoryStatus::Warning {
            current_gb: 1.2,
            threshold_gb: 1.0,
        };
        assert!(!warning.is_critical());
        assert!(warning.is_concerning());

        let critical = MemoryStatus::Critical {
            current_gb: 1.8,
            threshold_gb: 1.5,
        };
        assert!(critical.is_critical());
        assert!(critical.is_concerning());
    }
}
