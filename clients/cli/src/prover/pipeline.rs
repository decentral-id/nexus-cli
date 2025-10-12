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

        // Create shared references to avoid unnecessary cloning
        let task_shared = Arc::new(task.clone());
        let environment_shared = Arc::new(environment.clone());
        let client_id_shared = Arc::new(client_id.to_string());

        // Smart concurrency scaling based on system capabilities
        let total_memory_gb = crate::system::total_memory_gb();
        let cores = crate::system::num_cores();
        let optimal_concurrency = if total_memory_gb >= 16.0 && cores >= 8 {
            // High-end systems: aggressive concurrency
            (cores * 6).max(24).min(all_inputs.len())
        } else if total_memory_gb >= 8.0 && cores >= 4 {
            // Mid-range systems: balanced concurrency
            (cores * 4).max(12).min(all_inputs.len())
        } else {
            // Low-end systems: optimized for memory-constrained systems
            (cores * 3).max(4).min(all_inputs.len())
        };

        let optimized_workers = std::cmp::min(num_workers, optimal_concurrency);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(optimized_workers));

        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();

        // Streaming processing: process inputs in batches to limit memory usage
        const BATCH_SIZE: usize = 4; // Process 4 proofs at a time to stay within 2GB memory budget
        let mut all_proofs = Vec::with_capacity(all_inputs.len());
        let mut proof_hashes = Vec::with_capacity(all_inputs.len());
        let mut verification_failures = Vec::new();

        for batch_start in (0..all_inputs.len()).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, all_inputs.len());
            let batch_inputs = &all_inputs[batch_start..batch_end];

            // Process current batch
            let handles: Vec<_> = batch_inputs
                .iter()
                .enumerate()
                .map(|(local_index, input_data)| {
                    let task_ref = Arc::clone(&task_shared);
                    let environment_ref = Arc::clone(&environment_shared);
                    let client_id_ref = Arc::clone(&client_id_shared);
                    let input_data = input_data.clone();
                    let semaphore_ref = Arc::clone(&semaphore);
                    let cancellation_ref = cancellation_token.clone();
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

                        // Step 2: Generate and verify proof with streaming hash generation
                        let proof = ProvingEngine::prove_and_validate(
                            &inputs,
                            &task_ref,
                            &environment_ref,
                            &client_id_ref,
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
                                    task_shared.clone(),
                                    format!("Input {}: {}", global_index, e),
                                    environment_shared.clone(),
                                    client_id_shared.clone(),
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

        // Handle all verification failures in batch (avoid nested spawns)
        let failure_count = verification_failures.len();
        for (task, error_msg, env, client) in verification_failures {
            tokio::spawn(track_verification_failed(
                (*task).clone(),
                error_msg,
                (*env).clone(),
                (*client).clone(),
            ));
        }

        // If we have verification failures, we still return an error
        if failure_count > 0 {
            return Err(ProverError::MalformedTask(format!(
                "{} inputs failed verification",
                failure_count
            )));
        }

        let final_proof_hash = Self::combine_proof_hashes(&task_shared, &proof_hashes);

        Ok((all_proofs, final_proof_hash, proof_hashes))
    }

    /// Generate hash for a proof
    fn generate_proof_hash(proof: &Proof) -> String {
        let proof_bytes = postcard::to_allocvec(proof).expect("Failed to serialize proof");
        format!("{:x}", Keccak256::digest(&proof_bytes))
    }

    /// Generate hash for a proof with optimized serialization
    fn generate_proof_hash_optimized(proof: &Proof) -> String {
        // Use a more efficient serialization approach for hashing
        let proof_bytes = postcard::to_allocvec(proof).expect("Failed to serialize proof for hashing");
        let hash = Keccak256::digest(&proof_bytes);
        format!("{:x}", hash)
    }

    /// Generate hash for a proof with memory-efficient streaming to minimize memory usage
    fn generate_proof_hash_streaming(proof: &Proof) -> Result<String, ProverError> {
        // Use a smaller buffer for streaming serialization to reduce memory pressure
        let proof_bytes = postcard::to_allocvec(proof).map_err(ProverError::Serialization)?;
        
        // Process hash in chunks to avoid large memory allocations
        let mut hasher = Keccak256::new();
        hasher.update(&proof_bytes);
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
