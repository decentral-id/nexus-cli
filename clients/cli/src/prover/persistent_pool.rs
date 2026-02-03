//! Persistent process pool for massive subprocess performance improvement

use super::engine::ProvingEngine;
use super::types::ProverError;
use nexus_sdk::stwo::seq::Proof;
use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

/// A pre-warmed subprocess ready for ultra-fast proof generation
#[derive(Debug)]
pub struct PersistentProverProcess {
    child: Child,
    stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    last_used: Instant,
    proof_count: u64,
    created_at: Instant,
}

impl PersistentProverProcess {
    /// Create a new persistent subprocess process
    pub async fn new() -> Result<Self, ProverError> {
        let exe_path = std::env::current_exe()?;
        let mut cmd = Command::new(exe_path);
        cmd.arg("prove-fib-subprocess")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        // Apply maximum performance optimizations
        ProvingEngine::apply_performance_optimizations(&mut cmd);

        let mut child = cmd.spawn()?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ProverError::Subprocess("Failed to open stdin".to_string()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProverError::Subprocess("Failed to open stdout".to_string()))?;

        Ok(Self {
            child,
            stdin,
            stdout,
            last_used: Instant::now(),
            proof_count: 0,
            created_at: Instant::now(),
        })
    }

    /// Generate proof directly using the persistent process (no subprocess creation!)
    pub async fn prove_direct(&mut self, inputs: &(u32, u32, u32)) -> Result<Proof, ProverError> {
        // Ultra-fast direct I/O (no subprocess creation!)
        let mut buffer = [0u8; 12];
        buffer[0..4].copy_from_slice(&inputs.0.to_le_bytes());
        buffer[4..8].copy_from_slice(&inputs.1.to_le_bytes());
        buffer[8..12].copy_from_slice(&inputs.2.to_le_bytes());

        self.stdin.write_all(&buffer).await?;
        self.stdin.flush().await?;

        // Read proof directly
        let mut proof_buffer = Vec::new();
        self.stdout.read_to_end(&mut proof_buffer).await?;

        // Check process health
        match self.child.try_wait()? {
            Some(status) if !status.success() => {
                return Err(ProverError::Subprocess(format!("Process died: {}", status)));
            }
            Some(_) => {
                // Process completed but we shouldn't get here normally
                return Err(ProverError::Subprocess(
                    "Process ended unexpectedly".to_string(),
                ));
            }
            None => {
                // Process is still running (expected)
            }
        }

        self.last_used = Instant::now();
        self.proof_count += 1;

        // Zero-copy deserialization
        postcard::from_bytes(&proof_buffer)
            .map_err(|e| ProverError::Subprocess(format!("Deserialization failed: {}", e)))
    }

    /// Check if the process is healthy and should be kept in the pool
    pub fn is_healthy(&self) -> bool {
        let age = self.created_at.elapsed();
        let idle_time = self.last_used.elapsed();
        let max_age = Duration::from_secs(600); // 10 minutes max lifetime
        let max_idle = Duration::from_secs(300); // 5 minutes max idle time
        let max_proofs = 1000; // Max proofs per process

        age < max_age && idle_time < max_idle && self.proof_count < max_proofs
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> ProcessStats {
        ProcessStats {
            proof_count: self.proof_count,
            age_seconds: self.created_at.elapsed().as_secs(),
            idle_seconds: self.last_used.elapsed().as_secs(),
            is_healthy: self.is_healthy(),
        }
    }
}

/// Performance statistics for a persistent process
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProcessStats {
    pub proof_count: u64,
    pub age_seconds: u64,
    pub idle_seconds: u64,
    pub is_healthy: bool,
}

/// Pool of persistent subprocesses for ultra-fast proof generation
pub struct PersistentProcessPool {
    processes: Arc<Mutex<VecDeque<PersistentProverProcess>>>,
    max_size: usize,
    stats: Arc<Mutex<PoolStats>>,
}

#[derive(Debug, Default, Clone)]
pub struct PoolStats {
    pub total_processes_created: u64,
    pub processes_reused: u64,
    pub processes_expired: u64,
    pub active_processes: usize,
    pub total_proofs_generated: u64,
}

impl PersistentProcessPool {
    /// Create a new persistent process pool
    pub fn new(max_size: usize) -> Self {
        Self {
            processes: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            stats: Arc::new(Mutex::new(PoolStats::default())),
        }
    }

    /// Calculate safe pool size based on available system memory and requested workers
    pub fn calculate_safe_pool_size(requested_workers: usize, aggressive: bool) -> usize {
        let sys = sysinfo::System::new_all();
        let total_memory = sys.total_memory();
        let total_memory_gb = total_memory as f64 / 1024.0 / 1024.0 / 1024.0;
        let cores = num_cpus::get();

        // Base limits based on memory
        let memory_limit = if total_memory_gb < 1.5 {
            1
        } else if total_memory_gb < 2.0 {
            2
        } else if total_memory_gb < 4.0 {
            if aggressive {
                cores.min(4)
            } else {
                cores.min(2)
            }
        } else if total_memory_gb < 8.0 {
            if aggressive {
                cores.min(8)
            } else {
                cores.min(4)
            }
        } else if total_memory_gb < 16.0 {
            if aggressive {
                cores.min(12)
            } else {
                cores.min(8)
            }
        } else {
            if aggressive {
                cores.min(16)
            } else {
                cores.min(12)
            }
        };

        // Respect requested workers but cap at memory limit
        requested_workers.min(memory_limit).max(1)
    }

    /// Get a process from the pool, creating a new one if needed
    pub async fn get_process(&self) -> Result<PersistentProcessGuard, ProverError> {
        // Try to get healthy process from pool
        {
            let mut processes = self.processes.lock().unwrap();

            // Find first healthy process
            while let Some(mut process) = processes.pop_front() {
                if process.is_healthy() {
                    let mut stats = self.stats.lock().unwrap();
                    stats.processes_reused += 1;
                    stats.active_processes += 1;

                    return Ok(PersistentProcessGuard {
                        process: Some(process),
                        pool: Arc::clone(&self.processes),
                        stats: Arc::clone(&self.stats),
                    });
                } else {
                    // Process is unhealthy
                    let mut stats = self.stats.lock().unwrap();
                    stats.processes_expired += 1;
                    stats.active_processes = stats.active_processes.saturating_sub(1);

                    // Try to kill the unhealthy process
                    let _ = process.child.kill();
                    let _ = process.child.wait();
                }
            }
        }

        // No healthy process available, create new one
        drop(self.processes.lock().unwrap());
        let process = PersistentProverProcess::new().await?;

        let mut stats = self.stats.lock().unwrap();
        stats.total_processes_created += 1;
        stats.active_processes += 1;

        Ok(PersistentProcessGuard {
            process: Some(process),
            pool: Arc::clone(&self.processes),
            stats: Arc::clone(&self.stats),
        })
    }

    /// Pre-warm the pool with the specified number of processes
    pub async fn pre_warm(&self, count: usize) -> Result<(), ProverError> {
        let pre_warm_count = count.min(self.max_size);

        for _ in 0..pre_warm_count {
            let process = PersistentProverProcess::new().await?;
            let mut processes = self.processes.lock().unwrap();
            processes.push_back(process);

            let mut stats = self.stats.lock().unwrap();
            stats.total_processes_created += 1;
        }

        eprintln!("Pre-warmed {} persistent processes in pool", pre_warm_count);
        Ok(())
    }

    /// Clean up expired processes from the pool
    #[allow(dead_code)]
    pub async fn cleanup_expired(&self) -> Result<usize, ProverError> {
        let mut expired_count = 0usize;

        {
            let mut processes = self.processes.lock().unwrap();
            let mut i = 0;

            while i < processes.len() {
                if !processes[i].is_healthy() {
                    let _process = processes.remove(i); // Remove expired process

                    expired_count += 1;
                    // Don't increment i, since we removed element at i
                } else {
                    i += 1;
                }
            }
        }

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.processes_expired += expired_count as u64;
            stats.active_processes = stats.active_processes.saturating_sub(expired_count);
        }

        Ok(expired_count)
    }

    /// Get pool statistics
    pub fn get_stats(&self) -> PoolStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get detailed pool information
    #[allow(dead_code)]
    pub fn get_detailed_stats(&self) -> DetailedPoolStats {
        let processes = self.processes.lock().unwrap();
        let stats = self.stats.lock().unwrap();

        DetailedPoolStats {
            pool_size: processes.len(),
            max_size: self.max_size,
            stats: stats.clone(),
            process_details: processes.iter().map(|p| p.get_stats()).collect(),
        }
    }
}

/// Detailed pool statistics for monitoring
#[derive(Debug)]
#[allow(dead_code)]
pub struct DetailedPoolStats {
    pub pool_size: usize,
    pub max_size: usize,
    pub stats: PoolStats,
    pub process_details: Vec<ProcessStats>,
}

/// RAII guard to return process to pool when dropped
pub struct PersistentProcessGuard {
    process: Option<PersistentProverProcess>,
    pool: Arc<Mutex<VecDeque<PersistentProverProcess>>>,
    stats: Arc<Mutex<PoolStats>>,
}

impl std::ops::Deref for PersistentProcessGuard {
    type Target = PersistentProverProcess;

    fn deref(&self) -> &Self::Target {
        self.process.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PersistentProcessGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.process.as_mut().unwrap()
    }
}

impl PersistentProcessGuard {
    /// Generate a proof using this persistent process
    pub async fn prove(&mut self, inputs: &(u32, u32, u32)) -> Result<Proof, ProverError> {
        self.process.as_mut().unwrap().prove_direct(inputs).await
    }

    #[allow(dead_code)]
    pub fn process(&self) -> &PersistentProverProcess {
        self.process.as_ref().unwrap()
    }
}

impl Drop for PersistentProcessGuard {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            // Add proof generation to total count
            let proof_count = process.proof_count;

            if process.is_healthy() {
                let mut pool = self.pool.lock().unwrap();
                if pool.len() < 20 {
                    // Don't let pool grow too large
                    pool.push_back(process);

                    let mut stats = self.stats.lock().unwrap();
                    stats.total_proofs_generated += proof_count;
                } else {
                    // If pool is full, kill the process instead of returning it
                    let _ = process.child.kill();
                    let _ = process.child.wait(); // Wait for it to terminate
                }
            } else {
                // Process is unhealthy, clean it up synchronously
                let _ = process.child.kill();
                let _ = process.child.wait();

                let mut stats = self.stats.lock().unwrap();
                stats.processes_expired += 1;
                stats.active_processes = stats.active_processes.saturating_sub(1);
            }
        }
    }
}

use tokio::sync::RwLock;

/// Global persistent process pool
pub static GLOBAL_PROCESS_POOL: RwLock<Option<PersistentProcessPool>> = RwLock::const_new(None);

/// Initialize the global process pool
pub async fn initialize_global_process_pool(
    num_workers: usize,
    aggressive: bool,
) -> Result<(), ProverError> {
    // Calculate appropriate pool size
    let pool_size = PersistentProcessPool::calculate_safe_pool_size(num_workers, aggressive);

    // Create new pool
    let pool = PersistentProcessPool::new(pool_size);

    // Pre-warm processes if appropriate
    let sys = sysinfo::System::new_all();
    let total_memory = sys.total_memory();
    let total_memory_gb = total_memory as f64 / 1024.0 / 1024.0 / 1024.0;
    let cores = num_cpus::get();

    // Aggressive pre-warming logic
    let pre_warm_count = if aggressive && total_memory >= 8 * 1024 * 1024 * 1024 {
        let warm_count = if total_memory >= 16 * 1024 * 1024 * 1024 {
            (pool_size * 3) / 4 // 75%
        } else {
            pool_size / 2 // 50%
        };
        // Don't pre-warm more than 8 to avoid massive startup spike
        warm_count.min(8).max(1)
    } else if total_memory >= 4 * 1024 * 1024 * 1024 {
        // Modest pre-warming for medium-low memory
        (cores / 2).max(1).min(2)
    } else {
        // Minimal or no pre-warming for low memory
        if aggressive { 1 } else { 0 }
    };

    if pre_warm_count > 0 {
        eprintln!("Pre-warming {} process(es) in pool...", pre_warm_count);
        pool.pre_warm(pre_warm_count).await?;
    } else {
        eprintln!(
            "Low memory detected ({:.1}GB) - skipping pool pre-warming to conserve RAM",
            total_memory_gb
        );
    }

    let mut guard = GLOBAL_PROCESS_POOL.write().await;
    *guard = Some(pool);

    Ok(())
}

/// Get access to a process from the global pool
pub async fn get_process() -> Result<PersistentProcessGuard, ProverError> {
    // Ensure pool is initialized (lazy initialization if needed)
    {
        let guard = GLOBAL_PROCESS_POOL.read().await;
        if let Some(pool) = guard.as_ref() {
            return pool.get_process().await;
        }
    }

    // Initialize if empty (fallback)
    let mut guard = GLOBAL_PROCESS_POOL.write().await;
    if guard.is_none() {
        // Default conservative initialization if called without explicit init
        let pool = PersistentProcessPool::new(4);
        *guard = Some(pool);
    }

    guard.as_ref().unwrap().get_process().await
}

#[allow(dead_code)]
pub async fn get_global_pool_stats() -> Option<PoolStats> {
    let guard = GLOBAL_PROCESS_POOL.read().await;
    guard.as_ref().map(|pool| pool.get_stats())
}
