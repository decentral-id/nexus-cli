//! Proving pipeline that orchestrates the full proving process

use std::sync::Arc;
use super::engine::ProvingEngine;
use super::input::InputParser;
use super::memory_pool::{MemoryPool, BatchArena, SerializationBuffer, PROOF_BUFFER_POOL, HASH_BUFFER_POOL};
use super::process_pool::{ProcessGuard, PROCESS_POOL, initialize_process_pool};
use super::types::ProverError;
use crate::analytics::track_verification_failed;
use crate::environment::Environment;
use crate::task::Task;
use futures::future::join_all;
use nexus_sdk::stwo::seq::Proof;
use sha3::{Digest, Keccak256};
use tokio_util::sync::CancellationToken;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Orchestrates the complete proving pipeline
pub struct ProvingPipeline;

// Global initialization flag
static POOL_INITIALIZED: std::sync::Once = std::sync::Once::new();

impl ProvingPipeline {
    /// Execute authenticated proving for a task
    pub async fn prove_authenticated(
        task: &Task,
        environment: &Environment,
        client_id: &str,
        num_workers: usize,
    ) -> Result<(Vec<Proof>, String, Vec<String>), ProverError> {
        // Initialize process pool once
        POOL_INITIALIZED.call_once(|| {
            tokio::spawn(async {
                if let Err(e) = initialize_process_pool().await {
                    eprintln!("Warning: Failed to initialize process pool: {}", e);
                }
            });
        });
        match task.program_id.as_str() {
            "fib_input_initial" => {
                Self::prove_fib_task(task, environment, client_id, num_workers).await
            }
            _ => Err(ProverError::MalformedTask(format!(
                "Unsupported program ID: {}",
                task.program_id
            ))),
        }
    }

    /// Process fibonacci proving task with optimized single-task performance
    async fn prove_fib_task(
        task: &Task,
        environment: &Environment,
        client_id: &str,
        num_workers: usize,
    ) -> Result<(Vec<Proof>, String, Vec<String>), ProverError> {
        let all_inputs = task.all_inputs();

        if all_inputs.is_empty() {
            return Err(ProverError::MalformedTask(
                "No inputs provided for task".to_string(),
            ));
        }

        // Optimized for single-task performance since server limits to 1 task/120s
        let cores = crate::system::num_cores();
        let total_memory_gb = crate::system::total_memory_gb();

        // Optimized concurrency for single task completion speed
        let max_concurrency = if total_memory_gb >= 32.0 {
            cores * 4  // Reduced from 8x to focus on single-task efficiency
        } else if total_memory_gb >= 16.0 {
            cores * 3  // Reduced from 6x
        } else if total_memory_gb >= 8.0 {
            cores * 2  // Reduced from 4x
        } else {
            cores.max(1) // At least 1 worker
        };

        // Cap at reasonable limit for single task
        let optimized_workers = std::cmp::min(num_workers, max_concurrency).min(all_inputs.len());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(optimized_workers));

        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();

        // Optimized batch sizing for single-task completion
        let batch_size = if total_memory_gb >= 32.0 {
            std::cmp::min(20, all_inputs.len()) // Reduced from 50 for better single-task focus
        } else if total_memory_gb >= 16.0 {
            std::cmp::min(12, all_inputs.len()) // Reduced from 25
        } else if total_memory_gb >= 8.0 {
            std::cmp::min(8, all_inputs.len())  // Reduced from 15
        } else {
            std::cmp::min(4, all_inputs.len())  // Reduced from 8
        };

        // Use batch arena for memory efficiency
        let mut arena = BatchArena::new();
        let mut all_proofs = Vec::with_capacity(all_inputs.len());
        let mut proof_hashes = Vec::with_capacity(all_inputs.len());
        let mut verification_failures = Vec::new();

        for batch_start in (0..all_inputs.len()).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size, all_inputs.len());
            let batch_inputs = &all_inputs[batch_start..batch_end];

            // Clone shared data for this batch to reduce per-iteration overhead
            let batch_task = task.clone();
            let batch_environment = environment.clone();
            let batch_client_id = client_id.to_string();

            // Process current batch with optimized memory management and process pool
            let handles: Vec<_> = batch_inputs
                .iter()
                .enumerate()
                .map(|(local_index, input_data)| {
                    let input_data = input_data.clone();
                    let semaphore_ref = Arc::clone(&semaphore);
                    let cancellation_ref = cancellation_token.clone();
                    let task_ref = batch_task.clone();
                    let env_ref = batch_environment.clone();
                    let client_ref = batch_client_id.clone();
                    let global_index = batch_start + local_index;

                    tokio::spawn(async move {
                        // Check for cancellation before starting
                        if cancellation_ref.is_cancelled() {
                            return Err(ProverError::MalformedTask("Task cancelled".to_string()));
                        }

                        // Acquire a permit from the semaphore. This waits if the limit is reached.
                        let _permit = semaphore_ref.acquire_owned().await;

                        // Check for cancellation after acquiring permit
                        if cancellation_ref.is_cancelled() {
                            return Err(ProverError::MalformedTask("Task cancelled".to_string()));
                        }

                        // Step 1: Parse and validate input
                        let inputs = InputParser::parse_triple_input(&input_data)?;

                        // Step 2: Generate proof using pre-warmed process pool
                        let proof = Self::prove_with_process_pool(
                            &inputs,
                            &task_ref,
                            &env_ref,
                            &client_ref,
                        )
                        .await?;

                        // Step 3: Generate proof hash with zero-allocation streaming
                        let proof_hash = Self::generate_proof_hash_streaming(&proof)?;

                        Ok((proof, proof_hash, global_index))
                    })
                })
                .collect();

            // Wait for batch completion
            let results = join_all(handles).await;

            // Process batch results immediately to free memory
            for (result_index, result) in results.into_iter().enumerate() {
                let global_index = batch_start + result_index;
                match result {
                    Ok(Ok((proof, proof_hash, _))) => {
                        all_proofs.push(proof);
                        proof_hashes.push(proof_hash);
                    }
                    Ok(Err(e)) => {
                        // Collect verification failures for batch processing
                        match e {
                            ProverError::Stwo(_) | ProverError::GuestProgram(_) => {
                                verification_failures.push((
                                    batch_task.clone(),
                                    format!("Input {}: {}", global_index, e),
                                    batch_environment.clone(),
                                    batch_client_id.clone(),
                                ));
                            }
                            _ => {
                                // Cancel remaining tasks on critical errors
                                cancellation_token.cancel();
                                return Err(e);
                            }
                        }
                    }
                    Err(join_error) => {
                        return Err(ProverError::JoinError(join_error));
                    }
                }
            }

            // Force memory cleanup between batches
            tokio::task::yield_now().await;
        }

        // Optimize analytics tracking - batch failures to avoid task spawning overhead
        let failure_count = verification_failures.len();
        if failure_count > 0 {
            // Collect all failure data for batch processing
            let batch_failures: Vec<_> = verification_failures.into_iter().collect();

            // Fire-and-forget analytics with minimal overhead
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

        // If we have verification failures, we still return an error
        if failure_count > 0 {
            return Err(ProverError::MalformedTask(format!(
                "{} inputs failed verification",
                failure_count
            )));
        }

        // Use optimized reference for hash combination
        let final_proof_hash = Self::combine_proof_hashes(&task, &proof_hashes);

        Ok((all_proofs, final_proof_hash, proof_hashes))
    }

    /// Generate proof using pre-warmed process pool for maximum speed
    async fn prove_with_process_pool(
        inputs: &(u32, u32, u32),
        task: &Task,
        environment: &Environment,
        client_id: &str,
    ) -> Result<nexus_sdk::stwo::seq::Proof, ProverError> {
        // Try to get a pre-warmed process first
        match PROCESS_POOL.get_process().await {
            Ok(mut process_guard) => {
                // Use pre-warmed process for faster execution
                let proof_bytes = process_guard.execute_proof(inputs).await?;

                // Deserialize proof using zero-copy approach
                let proof: nexus_sdk::stwo::seq::Proof = postcard::from_bytes(&proof_bytes)
                    .map_err(|e| ProverError::Subprocess(
                        format!("Failed to deserialize proof from process pool: {}", e)
                    ))?;

                Ok(proof)
            }
            Err(e) => {
                // Fallback to regular proving if process pool fails
                eprintln!("Warning: Process pool failed, falling back: {}", e);
                ProvingEngine::prove_and_validate(inputs, task, environment, client_id).await
            }
        }
    }

    /// Generate hash for a proof with zero-allocation streaming and memory pool
    fn generate_proof_hash_streaming(proof: &Proof) -> Result<String, ProverError> {
        // Try to get a buffer from the pool first
        let mut hasher = if let Some(mut buffer) = HASH_BUFFER_POOL.acquire() {
            // Use pooled buffer and create hasher
            buffer.clear();
            let mut hasher = Keccak256::new();

            // Serialize proof directly into hasher - no intermediate Vec allocation
            postcard::to_io(proof, &mut hasher).map_err(ProverError::Serialization)?;

            // Return buffer to pool
            HASH_BUFFER_POOL.release(buffer);
            hasher
        } else {
            // Fallback: create new hasher
            let mut hasher = Keccak256::new();
            postcard::to_io(proof, &mut hasher).map_err(ProverError::Serialization)?;
            hasher
        };

        let hash = hasher.finalize();
        Ok(format!("{:x}", hash))
    }

    /// Generate hash for a proof with memory pool optimization
    #[allow(dead_code)] // Alternative implementation kept for reference
    fn generate_proof_hash_pooled(proof: &Proof) -> Result<String, ProverError> {
        // Try to get a serialization buffer from the pool
        if let Some(mut buffer) = PROOF_BUFFER_POOL.acquire() {
            buffer.clear();

            // Serialize into pooled buffer
            postcard::to_io(proof, &mut buffer).map_err(ProverError::Serialization)?;

            let hash = Keccak256::digest(&buffer);
            let result = format!("{:x}", hash);

            // Return buffer to pool
            PROOF_BUFFER_POOL.release(buffer);

            Ok(result)
        } else {
            // Fallback to standard method
            Self::generate_proof_hash_streaming(proof)
        }
    }

    /// Combine multiple proof hashes based on task type
    fn combine_proof_hashes(task: &Task, proof_hashes: &[String]) -> String {
        match task.task_type {
            crate::nexus_orchestrator::TaskType::AllProofHashes
            | crate::nexus_orchestrator::TaskType::ProofHash => {
                Task::combine_proof_hashes(proof_hashes)
            }
            _ => proof_hashes.first().cloned().unwrap_or_default(),
        }
    }
}
