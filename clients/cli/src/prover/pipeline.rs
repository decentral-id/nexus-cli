//! Proving pipeline that orchestrates the full proving process

#![allow(dead_code)]

use std::time::Instant;
use std::cell::RefCell;
use super::input::InputParser;
use super::adaptive_batch::get_global_batcher;
use super::types::ProverError;
use crate::analytics::track_verification_failed;
use crate::environment::Environment;
use crate::task::Task;
use futures::future::join_all;
use nexus_sdk::stwo::seq::Proof;
use sha3::{Digest, Keccak256};
use hex;

// Thread-local hash buffer for ultra-optimized hashing (no heap allocation!)
thread_local! {
    static HASH_BUFFER: RefCell<[u8; 32]> = RefCell::new([0u8; 32]);
}

/// Memory monitoring helper for low-memory systems
fn log_memory_usage(context: &str) {
    if let Ok(memory_usage) = std::fs::read_to_string("/proc/self/status") {
        if let Some(vmrss_line) = memory_usage.lines().find(|line| line.starts_with("VmRSS:")) {
            println!("[MEMORY] {}: {}", context, vmrss_line.trim());
        }
    }
}

/// Orchestrates the complete proving pipeline with optimizations
pub struct ProvingPipeline;

impl ProvingPipeline {
    /// Execute authenticated proving for a task
    pub async fn prove_authenticated(
        task: &Task,
        environment: &Environment,
        client_id: &str,
        num_workers: usize,
    ) -> Result<(Vec<Proof>, String, Vec<String>), ProverError> {
        let total_memory_gb = crate::system::total_memory_gb();

        // CRITICAL: Enhanced pre-task memory checks for low-memory systems
        if total_memory_gb <= 2.0 {
            log_memory_usage("Task boundary - before new task");

            // Check current memory usage before starting
            if let Ok(memory_usage) = std::fs::read_to_string("/proc/self/status") {
                if let Some(vmrss_line) = memory_usage.lines().find(|line| line.starts_with("VmRSS:")) {
                    if let Some(mb_str) = vmrss_line.split_whitespace().nth(1) {
                        if let Ok(memory_kb) = mb_str.parse::<usize>() {
                            let memory_mb = memory_kb / 1024;
                            let safety_threshold_mb = if total_memory_gb <= 1.5 { 800 } else { 1200 }; // Stricter for very low memory

                            if memory_kb > safety_threshold_mb * 1024 {
                                return Err(ProverError::Stwo(format!(
                                    "Memory too high: {} MB (threshold: {} MB) - refusing task to prevent OOM kill on {} GB system",
                                    memory_mb, safety_threshold_mb, total_memory_gb
                                )));
                            }

                            // Additional check: ensure we have enough headroom for the task
                            let all_inputs = task.all_inputs();
                            // With true subprocess isolation, we only need to ensure ONE proof can fit at a time
                            // Each proof runs in its own process and memory is reclaimed when the process exits
                            let estimated_memory_per_proof_mb = 400; // Conservative estimate for single proof (based on Stwo prover requirements)
                            let estimated_peak_memory_mb = memory_mb + estimated_memory_per_proof_mb;

                            // More conservative limit for t3.small due to observed memory accumulation
                            let memory_threshold_factor = if total_memory_gb <= 2.0 { 0.75 } else { 0.95 };
                            if estimated_peak_memory_mb > (total_memory_gb * 1024.0 * memory_threshold_factor) as usize {
                                return Err(ProverError::Stwo(format!(
                                    "Insufficient memory for single proof: estimated {} MB needed, only {} MB available on {} GB system ({}% threshold). Consider using larger instance.",
                                    estimated_peak_memory_mb,
                                    (total_memory_gb * 1024.0 * memory_threshold_factor) as usize,
                                    total_memory_gb,
                                    (memory_threshold_factor * 100.0) as usize
                                )));
                            }

                            // Show system capabilities on first task
                            static mut CAPABILITIES_SHOWN: bool = false;
                            if unsafe { !CAPABILITIES_SHOWN } {
                                println!("[SYSTEM] Sequential processing mode: can handle any number of inputs, one proof at a time");
                                println!("[SYSTEM] Memory per proof: ~{} MB, system limit: {} MB",
                                    estimated_memory_per_proof_mb, (total_memory_gb * 1024.0 * 0.95) as usize);
                                unsafe { CAPABILITIES_SHOWN = true; }
                            }

                            // Additional check for large tasks on low-memory systems
                            if all_inputs.len() > 10 && total_memory_gb <= 2.0 && memory_mb > 1100 {
                                return Err(ProverError::Stwo(format!(
                                    "Large task ({}) rejected for low-memory system with high current usage ({} MB). Memory accumulation detected - restart recommended.",
                                    all_inputs.len(), memory_mb
                                )));
                            }

                            println!("[MEMORY] Pre-task check passed: {} MB used, {} inputs will be processed sequentially (estimated ~{} MB per proof)",
                                memory_mb, all_inputs.len(), estimated_memory_per_proof_mb);
                        }
                    }
                }
            }
        }

        match task.program_id.as_str() {
            "fib_input_initial" => {
                Self::prove_fib_task_fully_isolated(task, environment, client_id, num_workers).await
            }
            _ => Err(ProverError::MalformedTask(format!(
                "Unsupported program ID: {}",
                task.program_id
            ))),
        }
    }

    /// Process fibonacci proving task with complete subprocess isolation for low-memory systems
    async fn prove_fib_task_fully_isolated(
        task: &Task,
        _environment: &Environment,
        _client_id: &str,
        _num_workers: usize,
    ) -> Result<(Vec<Proof>, String, Vec<String>), ProverError> {
        let all_inputs = task.all_inputs();

        if all_inputs.is_empty() {
            return Err(ProverError::MalformedTask(
                "No inputs provided for task".to_string(),
            ));
        }

        let total_memory_gb = crate::system::total_memory_gb();

        // CRITICAL: For low-memory systems, use sequential processing with immediate memory cleanup
        if total_memory_gb <= 2.0 {
            println!("[CRITICAL] Low-memory system detected - using sequential processing with immediate cleanup");
            log_memory_usage("Before sequential processing");

            let mut all_proofs = Vec::new();
            let mut proof_hashes = Vec::new();

            // Process inputs one by one to minimize memory usage with aggressive cleanup
            for (index, input_data) in all_inputs.iter().enumerate() {
                println!("[INFO] Processing proof {}/{} in low-memory mode", index + 1, all_inputs.len());

                // NOTE: Don't clear collections before proofs - we need to maintain all proofs for submission
                // The subprocess isolation should handle memory cleanup

                // Parse input
                let inputs = InputParser::parse_triple_input(input_data)?;

                // Generate proof using actual subprocess isolation
                let proof = Self::prove_with_true_subprocess_isolation(&inputs, task, _environment, _client_id).await?;

                // Generate hash
                let proof_hash = Self::generate_proof_hash_ultra_optimized(&proof)?;

                // Store results
                all_proofs.push(proof);
                proof_hashes.push(proof_hash);

                // NOTE: Don't clear proofs array during processing - it causes submission count mismatch
                // The actual subprocess isolation should handle memory cleanup properly now

                // Log memory usage after each proof
                if index == 0 || index % 3 == 0 {
                    log_memory_usage(&format!("After proof {}", index + 1));
                }
            }

            let final_proof_hash = Task::combine_proof_hashes(&proof_hashes);

            // Clear thread-local buffers to prevent accumulation between tasks
            HASH_BUFFER.with(|buffer_cell| {
                let mut buffer = buffer_cell.borrow_mut();
                buffer.fill(0);
            });

            log_memory_usage("Sequential processing completed");
            println!("[INFO] Low-memory processing completed: {} proofs processed", all_proofs.len());

            return Ok((all_proofs, final_proof_hash, proof_hashes));
        }

        // Normal processing for systems with sufficient memory (fallback)
        println!("[INFO] Sufficient memory detected - using normal processing");
        Self::prove_fib_task_normal(task, _environment, _client_id, _num_workers).await
    }

    /// Normal processing for systems with sufficient memory
    async fn prove_fib_task_normal(
        task: &Task,
        _environment: &Environment,
        _client_id: &str,
        _num_workers: usize,
    ) -> Result<(Vec<Proof>, String, Vec<String>), ProverError> {
        let all_inputs = task.all_inputs();

        if all_inputs.is_empty() {
            return Err(ProverError::MalformedTask(
                "No inputs provided for task".to_string(),
            ));
        }

        let _total_memory_gb = crate::system::total_memory_gb();

        // Use adaptive batching only for systems with more memory
        let (batch_size, use_adaptive_batching) = if super::adaptive_batch::should_use_global_batcher() {
            let batcher = get_global_batcher();
            (batcher.get_memory_adjusted_batch_size(all_inputs.len()).await, true)
        } else {
            // For low-memory systems, use simple fixed batching
            println!("[INFO] Using fixed batch size for low-memory system");
            (1, false) // Always use batch size 1 for low-memory systems
        };

        let mut all_proofs = Vec::with_capacity(all_inputs.len());
        let mut proof_hashes = Vec::with_capacity(all_inputs.len());
        let verification_failures = Vec::new();

        // Process inputs in batches with performance tracking
        for batch_start in (0..all_inputs.len()).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size, all_inputs.len());
            let batch_inputs = &all_inputs[batch_start..batch_end];
            let batch_start_time = Instant::now();

            // Multi-threaded processing for systems with more memory
            let handles: Vec<_> = batch_inputs
                .iter()
                .enumerate()
                .map(|(local_index, input_data)| {
                    let input_data = input_data.clone();
                    let global_index = batch_start + local_index;

                    tokio::spawn(async move {
                        // Step 1: Parse and validate input
                        let inputs = InputParser::parse_triple_input(&input_data)?;

                        // Step 2: Generate proof using isolated subprocess
                        let proof = Self::prove_with_isolated_process(&inputs).await?;

                        // Step 3: Generate proof hash with ultra-optimized thread-local buffer
                        let proof_hash = Self::generate_proof_hash_ultra_optimized(&proof)?;

                        Ok((proof, proof_hash, global_index))
                    })
                })
                .collect();

            // Wait for batch completion
            let results = join_all(handles).await;

            // Track batch performance for adaptive batching (only if not low-memory system)
            if use_adaptive_batching && super::adaptive_batch::should_use_global_batcher() {
                let batch_duration = batch_start_time.elapsed();
                let proofs_in_batch = results.len();
                let batcher = get_global_batcher();
                batcher.record_batch_performance(batch_size, batch_duration, proofs_in_batch).await;
            }

            // Process results
            for result in results {
                match result {
                    Ok(task_result) => {
                        match task_result {
                            Ok((proof, proof_hash, _global_index)) => {
                                all_proofs.push(proof);
                                proof_hashes.push(proof_hash);
                            }
                            Err(prover_error) => {
                                match prover_error {
                                    ProverError::Stwo(_) | ProverError::GuestProgram(_) => {
                                        eprintln!("Proof generation error: {}", prover_error);
                                    }
                                    _ => {
                                        eprintln!("Critical error in proof generation: {}", prover_error);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(join_error) => {
                        match join_error.try_into_panic() {
                            Ok(panic_payload) => {
                                let panic_msg = if let Some(s) = panic_payload.downcast_ref::<String>() {
                                    s.clone()
                                } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                                    s.to_string()
                                } else {
                                    "Unknown panic".to_string()
                                };
                                eprintln!("Task panicked: {}", panic_msg);
                                break;
                            }
                            Err(_) => {
                                eprintln!("Task was cancelled or failed to join");
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Fire-and-forget analytics with minimal overhead
        if !verification_failures.is_empty() {
            let batch_failures = verification_failures.clone();
            tokio::spawn(async move {
                for (task, error_msg, env, client) in batch_failures {
                    track_verification_failed(
                        task,
                        error_msg,
                        env,
                        client,
                    ).await;
                }
            });
        }

        // Use default hash combination for isolated subprocess
        let final_proof_hash = Task::combine_proof_hashes(&proof_hashes);

        // Clear thread-local buffers for low-memory systems to prevent accumulation
        if crate::system::total_memory_gb() <= 2.0 {
            HASH_BUFFER.with(|buffer_cell| {
                let mut buffer = buffer_cell.borrow_mut();
                buffer.fill(0);
            });
            log_memory_usage("After cleanup - thread-local buffers cleared");
        }

        Ok((all_proofs, final_proof_hash, proof_hashes))
    }

    /// Generate proof using true subprocess isolation for guaranteed memory cleanup
    async fn prove_with_true_subprocess_isolation(
        inputs: &(u32, u32, u32),
        task: &Task,
        environment: &Environment,
        client_id: &str,
    ) -> Result<nexus_sdk::stwo::seq::Proof, ProverError> {
        // Use actual subprocess isolation - memory gets reclaimed when process exits
        super::engine::ProvingEngine::prove_and_validate(inputs, task, environment, client_id).await
    }

    /// Generate proof using isolated subprocess for guaranteed memory cleanup (DEPRECATED - not actually isolated)
    async fn prove_with_isolated_process(
        inputs: &(u32, u32, u32),
    ) -> Result<nexus_sdk::stwo::seq::Proof, ProverError> {
        // This function is NOT actually isolated - it runs in the same process!
        // Use prove_with_true_subprocess_isolation instead
        super::engine::ProvingEngine::prove_fib_subprocess(inputs)
    }

    /// Generate hash for a proof with ultra-optimized thread-local buffer
    fn generate_proof_hash_ultra_optimized(proof: &Proof) -> Result<String, ProverError> {
        HASH_BUFFER.with(|buffer_cell| {
            let mut buffer = buffer_cell.borrow_mut();

            // Use stack buffer directly
            let mut hasher = Keccak256::new();
            postcard::to_io(proof, &mut hasher).map_err(ProverError::Serialization)?;

            let hash = hasher.finalize();
            buffer.copy_from_slice(&hash);

            Ok(hex::encode(buffer.as_slice()))
        })
    }

    /// Combine multiple proof hashes based on task type
    fn combine_proof_hashes(task: &Task, proof_hashes: &[String]) -> String {
        match task.task_type {
            crate::nexus_orchestrator::TaskType::AllProofHashes
            | crate::nexus_orchestrator::TaskType::ProofHash => {
                // Use all individual proof hashes
                proof_hashes.join("")
            }
            _ => {
                // Default combination for other task types
                Task::combine_proof_hashes(proof_hashes)
            }
        }
    }

    /// Collect all verification failures and report them
    async fn report_verification_failures(
        verification_failures: Vec<(Task, String, Environment, String)>,
    ) {
        if !verification_failures.is_empty() {
            // Fire-and-forget analytics with minimal overhead
            tokio::spawn(async move {
                for (task, error_msg, env, client) in verification_failures {
                    track_verification_failed(
                        task,
                        error_msg,
                        env,
                        client,
                    ).await;
                }
            });
        }
    }
}

/// Error collection for batch processing
#[derive(Debug)]
struct VerificationFailure {
    task: Task,
    error: String,
    environment: Environment,
    client_id: String,
}

/// Result of proof generation with hash
struct ProofResult {
    proof: Proof,
    hash: String,
    index: usize,
}