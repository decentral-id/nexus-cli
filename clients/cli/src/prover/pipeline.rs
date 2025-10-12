//! Proving pipeline that orchestrates the full proving process

use std::sync::Arc;
use super::engine::ProvingEngine;
use super::input::InputParser;
use super::types::ProverError;
use crate::analytics::track_verification_failed;
use crate::environment::Environment;
use crate::task::Task;
use futures::future::join_all;
use nexus_sdk::stwo::seq::Proof;
use sha3::{Digest, Keccak256};
use tokio_util::sync::CancellationToken;

/// Orchestrates the complete proving pipeline
pub struct ProvingPipeline;

impl ProvingPipeline {
    /// Execute authenticated proving for a task
    pub async fn prove_authenticated(
        task: &Task,
        environment: &Environment,
        client_id: &str,
        num_workers: usize,
    ) -> Result<(Vec<Proof>, String, Vec<String>), ProverError> {
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

    /// Process fibonacci proving task with multiple inputs using streaming memory optimization
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

      
        // Maximum parallelization: run many more subprocesses than CPU cores
        // Since each subprocess is I/O bound and mostly waits for SDK, we can over-subscribe
        let cores = crate::system::num_cores();
        let total_memory_gb = crate::system::total_memory_gb();

        // Aggressive concurrency scaling - much higher than core count
        let max_concurrency = if total_memory_gb >= 32.0 {
            // High-end systems: 8x cores for maximum throughput
            cores * 8
        } else if total_memory_gb >= 16.0 {
            // Mid-high systems: 6x cores
            cores * 6
        } else if total_memory_gb >= 8.0 {
            // Mid-range systems: 4x cores
            cores * 4
        } else {
            // Low-end systems: 2x cores
            cores * 2
        };

        // Cap at reasonable limit and available inputs
        let optimized_workers = std::cmp::min(num_workers, max_concurrency).min(all_inputs.len());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(optimized_workers));

        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();

        // Aggressive batch sizing - process much larger batches for maximum throughput
        let batch_size = if total_memory_gb >= 32.0 {
            std::cmp::min(50, all_inputs.len()) // High-end: up to 50 concurrent proofs
        } else if total_memory_gb >= 16.0 {
            std::cmp::min(25, all_inputs.len()) // Mid-high: up to 25 concurrent proofs
        } else if total_memory_gb >= 8.0 {
            std::cmp::min(15, all_inputs.len()) // Mid-range: up to 15 concurrent proofs
        } else {
            std::cmp::min(8, all_inputs.len())  // Low-end: up to 8 concurrent proofs
        };
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

            // Process current batch with optimized references
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

                        // Step 2: Generate proof with optimized reference sharing
                        let proof = ProvingEngine::prove_and_validate(
                            &inputs,
                            &task_ref,
                            &env_ref,
                            &client_ref,
                        )
                        .await?;

                        // Step 3: Generate proof hash with memory-efficient streaming
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

    /// Generate hash for a proof
    #[allow(dead_code)] // Alternative implementation kept for reference
    fn generate_proof_hash(proof: &Proof) -> String {
        let proof_bytes = postcard::to_allocvec(proof).expect("Failed to serialize proof");
        format!("{:x}", Keccak256::digest(&proof_bytes))
    }

    /// Generate hash for a proof with optimized serialization
    #[allow(dead_code)] // Alternative implementation kept for reference
    fn generate_proof_hash_optimized(proof: &Proof) -> String {
        // Use a more efficient serialization approach for hashing
        let proof_bytes = postcard::to_allocvec(proof).expect("Failed to serialize proof for hashing");
        let hash = Keccak256::digest(&proof_bytes);
        format!("{:x}", hash)
    }

    /// Generate hash for a proof with zero-allocation streaming for maximum performance
    fn generate_proof_hash_streaming(proof: &Proof) -> Result<String, ProverError> {
        // Use direct serialization with hasher to avoid intermediate allocation
        // This saves both memory allocation time and reduces memory pressure

        // Create hasher that can serialize directly
        let mut hasher = Keccak256::new();

        // Serialize proof directly into hasher - no intermediate Vec allocation
        // This is the most memory-efficient approach
        postcard::to_io(proof, &mut hasher).map_err(ProverError::Serialization)?;

        let hash = hasher.finalize();
        Ok(format!("{:x}", hash))
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
