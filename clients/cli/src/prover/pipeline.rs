//! Proving pipeline that orchestrates the full proving process

#![allow(dead_code)]

use super::input::InputParser;
use super::types::ProverError;
use crate::environment::Environment;
use crate::task::Task;
use nexus_sdk::stwo::seq::Proof;
use sha3::{Digest, Keccak256};
use hex;


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

/// Error collection for batch processing
#[allow(dead_code)]
#[derive(Debug)]
struct VerificationFailure {
    task: Task,
    error: String,
    environment: Environment,
    client_id: String,
}

/// Result of proof generation with hash
#[allow(dead_code)]
struct ProofResult {
    proof: Proof,
    hash: String,
    index: usize,
}

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
                            // Adaptive memory estimation based on available system memory
                            let estimated_memory_per_proof_mb = if total_memory_gb <= 1.0 {
                                180 // Ultra-aggressive for 1GB systems
                            } else if total_memory_gb <= 1.5 {
                                250 // Aggressive for 1.5GB systems
                            } else if total_memory_gb <= 2.0 {
                                325 // Moderate for 2GB systems
                            } else {
                                400 // Standard for larger systems
                            };
                            let estimated_peak_memory_mb = memory_mb + estimated_memory_per_proof_mb;

                            // Adaptive memory thresholds based on system size
                            let memory_threshold_factor = if total_memory_gb <= 1.0 { 0.65 } else if total_memory_gb <= 1.5 { 0.70 } else if total_memory_gb <= 2.0 { 0.75 } else { 0.95 };
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

    /// Process fibonacci proving task with multiple inputs (original simple implementation)
    async fn prove_fib_task_fully_isolated(
        task: &Task,
        environment: &Environment,
        client_id: &str,
        _num_workers: usize,
    ) -> Result<(Vec<Proof>, String, Vec<String>), ProverError> {
        let all_inputs = task.all_inputs();
        if all_inputs.is_empty() {
            return Err(ProverError::MalformedTask("No inputs provided for task".to_string()));
        }
        let mut all_proofs = Vec::new();
        let mut proof_hashes = Vec::new();
        for input_data in all_inputs.iter() {
            let inputs = InputParser::parse_triple_input(input_data)?;
            let proof = super::engine::ProvingEngine::prove_and_validate(&inputs, task, environment, client_id).await?;

            let proof_hash = Self::generate_proof_hash(&proof);
            all_proofs.push(proof);
            proof_hashes.push(proof_hash);
        }
        let final_proof_hash = Self::combine_proof_hashes(task, &proof_hashes);
        Ok((all_proofs, final_proof_hash, proof_hashes))
    }

    
    
    /// Generate hash for a proof
    fn generate_proof_hash(proof: &Proof) -> String {
        let mut hasher = Keccak256::new();
        let proof_bytes = postcard::to_allocvec(proof).unwrap();
        hasher.update(&proof_bytes);
        let hash = hasher.finalize();
        hex::encode(hash)
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
}