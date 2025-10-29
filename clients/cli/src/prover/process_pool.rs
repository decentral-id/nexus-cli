//! Pre-warmed process pool for eliminating subprocess startup overhead

use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::env;
use tokio::process::{Child, Command};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::prover::engine::ProvingEngine;
use crate::prover::types::ProverError;
use once_cell::sync::Lazy;

/// A pre-warmed subprocess ready for proof generation
struct PreWarmedProcess {
    child: Child,
    stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    last_used: std::time::Instant,
}

impl PreWarmedProcess {
    async fn new() -> Result<Self, ProverError> {
        let exe_path = env::current_exe()?;
        let mut cmd = Command::new(exe_path);
        cmd.arg("prove-fib-subprocess")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        // Apply performance optimizations
        ProvingEngine::apply_performance_optimizations(&mut cmd);

        let mut child = cmd.spawn()?;

        let stdin = child.stdin.take().ok_or_else(|| {
            ProverError::Subprocess("Failed to open stdin".to_string())
        })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            ProverError::Subprocess("Failed to open stdout".to_string())
        })?;

        Ok(Self {
            child,
            stdin,
            stdout,
            last_used: std::time::Instant::now(),
        })
    }

    async fn execute_proof(&mut self, inputs: &(u32, u32, u32)) -> Result<Vec<u8>, ProverError> {
        self.last_used = std::time::Instant::now();

        // Serialize inputs
        let input_bytes = postcard::to_allocvec(inputs)?;

        // Write inputs to stdin
        self.stdin.write_all(&input_bytes).await?;
        self.stdin.flush().await?;

        // Read proof from stdout
        let mut buffer = Vec::new();
        self.stdout.read_to_end(&mut buffer).await?;

        // Check process status
        match self.child.try_wait()? {
            Some(status) => {
                if !status.success() {
                    return Err(ProverError::Subprocess(format!(
                        "Pre-warmed process failed: {}", status
                    )));
                }
            }
            None => {
                // Process is still running (expected)
            }
        }

        Ok(buffer)
    }

    fn is_expired(&self, max_age: std::time::Duration) -> bool {
        self.last_used.elapsed() > max_age
    }
}

/// Pool of pre-warmed processes for rapid proof generation
pub struct ProcessPool {
    processes: Arc<Mutex<VecDeque<PreWarmedProcess>>>,
    max_size: usize,
    max_process_age: std::time::Duration,
}

impl ProcessPool {
    pub fn new(max_size: usize, max_process_age: std::time::Duration) -> Self {
        Self {
            processes: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            max_process_age,
        }
    }

    async fn add_process(&self) -> Result<(), ProverError> {
        let process = PreWarmedProcess::new().await?;
        let mut processes = self.processes.lock().unwrap();

        if processes.len() < self.max_size {
            processes.push_back(process);
        }
        // If pool is full, we discard the process (it will be cleaned up)

        Ok(())
    }

    pub async fn get_process(&self) -> Result<ProcessGuard, ProverError> {
        // Clean up expired processes first
        self.cleanup_expired().await?;

        // Try to get an existing process
        let process_opt: Option<PreWarmedProcess> = {
            let mut processes = self.processes.lock().unwrap();

            if let Some(mut process) = processes.pop_front() {
                // Check if process is still alive
                match process.child.try_wait() {
                    Ok(None) => {
                        // Process is still running, return it
                        return Ok(ProcessGuard {
                            process: Some(process),
                            pool: Arc::clone(&self.processes),
                        });
                    }
                    Ok(Some(_)) => {
                        // Process has died, continue to create new one
                        None
                    }
                    Err(_) => {
                        // Error checking status, create new process
                        None
                    }
                }
            } else {
                None
            }
        };

        // No available process, create a new one
        let process = PreWarmedProcess::new().await?;

        Ok(ProcessGuard {
            process: Some(process),
            pool: Arc::clone(&self.processes),
        })
    }

    async fn cleanup_expired(&self) -> Result<(), ProverError> {
        // Collect expired processes while holding the lock
        let expired_processes: Vec<_> = {
            let mut processes = self.processes.lock().unwrap();
            let mut expired = Vec::new();
            let mut i = 0;

            while i < processes.len() {
                if processes[i].is_expired(self.max_process_age) {
                    expired.push(processes.remove(i).unwrap());
                } else {
                    i += 1;
                }
            }
            expired
        };

        // Kill expired processes outside the lock
        for mut process in expired_processes {
            let _ = process.child.kill().await;
        }

        Ok(())
    }

    pub async fn pre_warm_pool(&self, count: usize) -> Result<(), ProverError> {
        for _ in 0..count.min(self.max_size) {
            if let Err(e) = self.add_process().await {
                eprintln!("Warning: Failed to pre-warm process: {}", e);
            }
        }
        Ok(())
    }
}

/// RAII guard that returns process to pool when dropped
pub struct ProcessGuard {
    process: Option<PreWarmedProcess>,
    pool: Arc<Mutex<VecDeque<PreWarmedProcess>>>,
}

impl ProcessGuard {
    pub async fn execute_proof(&mut self, inputs: &(u32, u32, u32)) -> Result<Vec<u8>, ProverError> {
        match &mut self.process {
            Some(process) => process.execute_proof(inputs).await,
            None => Err(ProverError::Subprocess("No process available".to_string())),
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            // Try to return process to pool if it's still healthy
            match process.child.try_wait() {
                Ok(None) => {
                    // Process is still running, return it to pool
                    if let Ok(mut pool) = self.pool.lock() {
                        if pool.len() < 50 { // Don't let pool grow too large
                            pool.push_back(process);
                        }
                    }
                }
                _ => {
                    // Process has died or error, let it be dropped
                }
            }
        }
    }
}

/// Global process pool instance
pub static PROCESS_POOL: Lazy<ProcessPool> = Lazy::new(|| {
    ProcessPool::new(
        10,  // Maximum 10 pre-warmed processes
        std::time::Duration::from_secs(300), // 5 minutes max age
    )
});

/// Initialize the process pool with pre-warmed processes
pub async fn initialize_process_pool() -> Result<(), ProverError> {
    // Pre-warm 2-4 processes depending on available memory
    let total_memory_gb = crate::system::total_memory_gb();
    let pre_warm_count = if total_memory_gb >= 16.0 {
        4
    } else if total_memory_gb >= 8.0 {
        2
    } else {
        1
    };

    PROCESS_POOL.pre_warm_pool(pre_warm_count).await
}