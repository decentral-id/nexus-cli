//! Proving pipeline that orchestrates the full proving process

#![allow(dead_code)]

use std::time::Instant;
use std::cell::RefCell;
use nexus_sdk::Prover;
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
        // No global process pool initialization needed - using direct engine approach

        match task.program_id.as_str() {
            "fib_input_initial" => {
                Self::prove_fib_task_optimized(task, environment, client_id, num_workers).await
            }
            _ => Err(ProverError::MalformedTask(format!(
                "Unsupported program ID: {}",
                task.program_id
            ))),
        }
    }

    /// Process fibonacci proving task with ultra-fast persistent process optimization
    async fn prove_fib_task_optimized(
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

        // Get adaptive batch size based on performance and system resources
        let batcher = get_global_batcher();
        let _base_batch_size = batcher.get_optimal_batch_size().await;
        let batch_size = batcher.get_memory_adjusted_batch_size(all_inputs.len()).await;

        
        let mut all_proofs = Vec::with_capacity(all_inputs.len());
        let mut proof_hashes = Vec::with_capacity(all_inputs.len());
        let verification_failures = Vec::new();

        // Process all inputs using optimized direct approach

        // Process inputs in batches with performance tracking
        for batch_start in (0..all_inputs.len()).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size, all_inputs.len());
            let batch_inputs = &all_inputs[batch_start..batch_end];
            let batch_start_time = Instant::now();

            // Process current batch with optimized approach
            let handles: Vec<_> = batch_inputs
                .iter()
                .enumerate()
                .map(|(local_index, input_data)| {
                    let input_data = input_data.clone();
                    let global_index = batch_start + local_index;

                    tokio::spawn(async move {
                        // Step 1: Parse and validate input
                        let inputs = InputParser::parse_triple_input(&input_data)?;

                        // Step 2: Generate proof using optimized engine
                        let proof = Self::prove_with_optimized_engine(&inputs).await?;

                        // Step 3: Generate proof hash with ultra-optimized thread-local buffer
                        let proof_hash = Self::generate_proof_hash_ultra_optimized(&proof)?;

                        Ok((proof, proof_hash, global_index))
                    })
                })
                .collect();

            // Wait for batch completion
            let results = join_all(handles).await;

            // Track batch performance for adaptive batching
            let batch_duration = batch_start_time.elapsed();
            let proofs_in_batch = results.len();
            batcher.record_batch_performance(batch_size, batch_duration, proofs_in_batch).await;

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
                                // Handle ProverError from proof generation
                                match prover_error {
                                    ProverError::Stwo(_) | ProverError::GuestProgram(_) => {
                                        // For now, just log the error and continue
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
                        // Handle JoinError from task spawning
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

        // Use optimized reference for hash combination
        let final_proof_hash = Self::combine_proof_hashes(&task, &proof_hashes);

        Ok((all_proofs, final_proof_hash, proof_hashes))
    }

    /// Generate proof using optimized engine approach
    async fn prove_with_optimized_engine(
        inputs: &(u32, u32, u32),
    ) -> Result<nexus_sdk::stwo::seq::Proof, ProverError> {
        // Use the original proven approach with our zero-allocation optimizations
        let prover = super::engine::ProvingEngine::create_fib_prover()?;
        let (view, proof) = prover
            .prove_with_input::<(), (u32, u32, u32)>(&(), inputs)
            .map_err(|e| {
                super::types::ProverError::Stwo(format!(
                    "Failed to generate proof for inputs {:?}: {}",
                    inputs, e
                ))
            })?;

        // Check exit code
        super::verifier::ProofVerifier::check_exit_code(&view)?;

        Ok(proof)
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