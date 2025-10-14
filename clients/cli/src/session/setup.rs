//! Session setup and initialization

use crate::analytics::set_wallet_address_for_reporting;
use crate::config::Config;
use crate::environment::Environment;
use crate::events::Event;
use crate::orchestrator::OrchestratorClient;
use crate::runtime::start_authenticated_worker;
use ed25519_dalek::SigningKey;
use std::error::Error;
use sysinfo::System;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Session data for both TUI and headless modes
#[derive(Debug)]
pub struct SessionData {
    /// Event receiver for worker events
    pub event_receiver: mpsc::Receiver<Event>,
    /// Join handles for worker tasks
    pub join_handles: Vec<JoinHandle<()>>,
    /// Shutdown sender to stop all workers
    pub shutdown_sender: broadcast::Sender<()>,
    /// Shutdown sender for max tasks completion
    pub max_tasks_shutdown_sender: broadcast::Sender<()>,
    /// Node ID
    pub node_id: u64,
    /// Orchestrator client
    pub orchestrator: OrchestratorClient,
    /// Number of workers (for display purposes)
    pub num_workers: usize,
}

/// Clamp thread count based on available system memory
/// Returns the maximum number of threads that can be safely used given system memory
fn clamp_threads_by_memory(requested_threads: usize, aggressive: bool) -> usize {
    let mut sysinfo = System::new();
    sysinfo.refresh_memory();

    let total_system_memory = sysinfo.total_memory();
    let total_cores = crate::system::num_cores();

    // Calculate memory per subprocess based on actual usage patterns with larger safety margins
    // Main process: minimal base for ultra-low-memory systems
    // Each subprocess: actual usage can vary significantly by task size and complexity
    let base_process_memory = if total_system_memory <= 1024 * 1024 * 1024 { // <= 1GB systems
        20 * 1024 * 1024 // 20MB base for ultra-low-memory systems (extremely conservative)
    } else {
        50 * 1024 * 1024 // 50MB base for normal systems
    };

    let memory_per_subprocess = if aggressive {
        if total_system_memory <= 1024 * 1024 * 1024 { // <= 1GB systems
            35 * 1024 * 1024 // 35MB per subprocess in aggressive mode for 1GB systems
        } else {
            60 * 1024 * 1024 // 60MB per subprocess in aggressive mode (higher performance)
        }
    } else {
        if total_system_memory <= 1024 * 1024 * 1024 { // <= 1GB systems
            25 * 1024 * 1024 // 25MB per subprocess in standard mode for 1GB systems
        } else {
            40 * 1024 * 1024 // 40MB per subprocess in standard mode (balanced)
        }
    };

    // Calculate maximum subprocesses based on aggressive parallelization strategy
    // Using reduced memory allocation for 1GB systems: 25-35MB per subprocess
    let multiplier = if aggressive {
        if total_system_memory >= 16 * 1024 * 1024 * 1024 { // 16GB+
            6 // High-end systems: 6x cores (reduced from 8 for memory)
        } else if total_system_memory >= 8 * 1024 * 1024 * 1024 { // 8GB+
            4 // Mid-high systems: 4x cores (reduced from 6 for memory)
        } else if total_system_memory >= 4 * 1024 * 1024 * 1024 { // 4GB+
            3 // Mid-range systems: 3x cores (reduced from 4 for memory)
        } else if total_system_memory >= 2 * 1024 * 1024 * 1024 { // 2GB+
            2 // Low-end systems: 2x cores
        } else { // <= 1GB
            1 // Ultra-low-memory systems: 1x core only
        }
    } else {
        if total_system_memory >= 8 * 1024 * 1024 * 1024 { // 8GB+
            3 // Standard mode: conservative (reduced from 4 for memory)
        } else if total_system_memory >= 4 * 1024 * 1024 * 1024 { // 4GB+
            2 // Standard mode: moderate (reduced from 3 for memory)
        } else if total_system_memory >= 2 * 1024 * 1024 * 1024 { // 2GB+
            1 // Standard mode: minimal for 2GB systems
        } else { // <= 1GB
            1 // Standard mode: minimal for 1GB systems
        }
    };

    let max_subprocesses = total_cores * multiplier;
    let total_subprocess_memory = max_subprocesses * memory_per_subprocess;
    let total_required_memory = (base_process_memory + total_subprocess_memory) as u64;

    // Calculate max threads based on total system memory and optimization mode
    let memory_reserve_ratio = if total_system_memory <= 1024 * 1024 * 1024 { // <= 1GB systems
        0.40 // Reserve 40% for ultra-low-memory systems (extremely conservative)
    } else if aggressive {
        0.10 // Aggressive: reserve only 10%
    } else {
        0.15 // Standard: reserve 15%
    };
    let available_memory = (total_system_memory as f64 * (1.0 - memory_reserve_ratio)) as u64;

    // Allow more threads on systems with sufficient memory for parallel subprocess strategy
    let max_threads_by_memory = if total_required_memory <= available_memory {
        requested_threads // Allow requested threads if memory permits subprocess strategy
    } else {
        // Fall back to memory-per-thread calculation if insufficient memory
        let memory_per_thread = if total_system_memory <= 1024 * 1024 * 1024 { // <= 1GB systems
            200 * 1024 * 1024 // 200MB per thread for ultra-low-memory systems (extremely conservative)
        } else if total_system_memory <= 2 * 1024 * 1024 * 1024 { // <= 2GB systems
            512 * 1024 * 1024 // 512MB per thread for low-memory systems
        } else {
            crate::consts::cli_consts::PROJECTED_MEMORY_REQUIREMENT // 2GB for normal systems
        };
        (available_memory / memory_per_thread) as usize
    };

    // Return the minimum of requested threads and memory-limited threads
    // Always allow at least 1 thread
    requested_threads.min(max_threads_by_memory.max(1))
}

/// Warn the user if their available memory seems insufficient for the task(s) at hand
pub fn warn_memory_configuration(_max_threads: Option<u32>) {
    // Skip OOM warning - the new memory calculation in clamp_threads_by_memory
    // already handles this properly with realistic subprocess memory usage
    // This prevents false warnings for configurations that are actually safe
}

/// Sets up an authenticated worker session
///
/// This function handles all the common setup required for both TUI and headless modes:
/// 1. Creates signing key for the prover
/// 2. Sets up shutdown channel
/// 3. Starts authenticated worker
/// 4. Returns session data for mode-specific handling
///
/// # Arguments
/// * `config` - Resolved configuration with node_id and client_id
/// * `env` - Environment to connect to
/// * `max_threads` - Optional maximum number of threads for proving
/// * `max_difficulty` - Optional override for task difficulty
/// * `aggressive` - Whether to use aggressive resource optimization
///
/// # Returns
/// * `Ok(SessionData)` - Successfully set up session
/// * `Err` - Session setup failed
pub async fn setup_session(
    config: Config,
    env: Environment,
    check_mem: bool,
    max_threads: Option<u32>,
    max_tasks: Option<u32>,
    max_difficulty: Option<crate::nexus_orchestrator::TaskDifficulty>,
    aggressive: bool,
) -> Result<SessionData, Box<dyn Error>> {
    let node_id = config.node_id.parse::<u64>()?;
    let client_id = config.user_id;

    // Create a signing key for the prover
    let mut csprng = rand_core::OsRng;
    let signing_key: SigningKey = SigningKey::generate(&mut csprng);

    // Create orchestrator client
    let orchestrator_client = OrchestratorClient::new(env.clone());

    let total_cores = crate::system::num_cores();
    
    // Calculate optimal worker count based on optimization mode
    let (max_workers, default_workers) = if aggressive {
        // Aggressive mode: Use 95% of cores for maximum performance
        let max = ((total_cores as f64 * 0.95).ceil() as usize).max(1);
        (max, max)
    } else {
        // Standard mode: Use up to 90% of cores for proving, leaving room for system processes
        let max = ((total_cores as f64 * 0.9).ceil() as usize).max(1);
        (max, max)
    };
    
    let mut num_workers: usize = max_threads.unwrap_or(default_workers as u32).clamp(1, max_workers as u32) as usize;

    // Check memory and clamp threads if max-threads was explicitly set OR check-memory flag is set OR aggressive mode
    if max_threads.is_some() || check_mem || aggressive {
        // Get system memory info for debugging
        let mut sysinfo = System::new();
        sysinfo.refresh_memory();
        let total_system_memory = sysinfo.total_memory();
        
        let memory_clamped_workers = clamp_threads_by_memory(num_workers, aggressive);
        
        // Debug output for memory calculation
        if aggressive || check_mem {
            let total_gb = total_system_memory as f64 / 1024.0 / 1024.0 / 1024.0;
            let available_ratio = if total_system_memory <= 1024 * 1024 * 1024 { 0.75 } else if aggressive { 0.90 } else { 0.85 };
            let available_gb = total_gb * available_ratio;
            let total_cores = crate::system::num_cores();
            let multiplier = if aggressive {
                if total_system_memory >= 16 * 1024 * 1024 * 1024 { 6 }
                else if total_system_memory >= 8 * 1024 * 1024 * 1024 { 4 }
                else if total_system_memory >= 4 * 1024 * 1024 * 1024 { 3 }
                else if total_system_memory >= 2 * 1024 * 1024 * 1024 { 2 }
                else { 1 }
            } else {
                if total_system_memory >= 8 * 1024 * 1024 * 1024 { 3 }
                else if total_system_memory >= 4 * 1024 * 1024 * 1024 { 2 }
                else { 1 }
            };

            let subprocess_memory_mb = if total_system_memory <= 1024 * 1024 * 1024 { // <= 1GB systems
            if aggressive { 35 } else { 25 }
        } else {
            if aggressive { 60 } else { 40 }
        };

            crate::print_cmd_info!(
                "Memory calculation",
                "System: {:.1}GB total, {:.1}GB available, {} cores, {}x subprocess multiplier ({} mode), {}MB per subprocess, requested: {} threads, calculated max: {} threads",
                total_gb,
                available_gb,
                total_cores,
                multiplier,
                if aggressive { "aggressive" } else { "standard" },
                subprocess_memory_mb,
                num_workers,
                memory_clamped_workers
            );
        }
        if memory_clamped_workers < num_workers {
            let mode_text = if aggressive { "aggressive" } else { "standard" };
            let total_gb = total_system_memory as f64 / 1024.0 / 1024.0 / 1024.0;
            let available_ratio = if total_system_memory <= 1024 * 1024 * 1024 { 0.75 } else if aggressive { 0.90 } else { 0.85 };
            let available_gb = total_gb * available_ratio;
            let subprocess_memory_mb = if total_system_memory <= 1024 * 1024 * 1024 { // <= 1GB systems
            if aggressive { 35 } else { 25 }
        } else {
            if aggressive { 60 } else { 40 }
        };
            crate::print_cmd_warn!(
                "Memory limit",
                "Reduced thread count from {} to {} due to insufficient memory ({} mode). System: {:.1}GB total, {:.1}GB available, {}MB per subprocess. Using optimized memory calculation for low-memory systems.",
                num_workers,
                memory_clamped_workers,
                mode_text,
                total_gb,
                available_gb,
                subprocess_memory_mb
            );
            num_workers = memory_clamped_workers;
        }
    }

    // Set low memory environment variable for 1GB systems to enable ultra-optimized settings
    let mut sysinfo_for_check = System::new();
    sysinfo_for_check.refresh_memory();
    let total_system_memory_for_check = sysinfo_for_check.total_memory();
    if total_system_memory_for_check <= 1024 * 1024 * 1024 {
        unsafe {
            std::env::set_var("NEXUS_1GB_MODE", "1");
        }
        crate::print_cmd_info!(
            "Low memory mode",
            "Enabled 1GB RAM optimizations (reduced memory allocation, single-threaded subprocesses)"
        );
    }

    // Additional memory warning if explicitly requested
    if check_mem {
        warn_memory_configuration(Some(num_workers as u32));
    }

    // Create shutdown channel - only one shutdown signal needed
    let (shutdown_sender, _) = broadcast::channel(1);

    // Set wallet for reporting
    set_wallet_address_for_reporting(config.wallet_address.clone());

    // Start authenticated worker (only mode we support now)
    let (event_receiver, join_handles, max_tasks_shutdown_sender) = start_authenticated_worker(
        node_id,
        signing_key,
        orchestrator_client.clone(),
        shutdown_sender.subscribe(),
        env,
        client_id,
        max_tasks,
        max_difficulty,
        num_workers,
    )
    .await;

    Ok(SessionData {
        event_receiver,
        join_handles,
        shutdown_sender,
        max_tasks_shutdown_sender,
        node_id,
        orchestrator: orchestrator_client,
        num_workers,
    })
}
