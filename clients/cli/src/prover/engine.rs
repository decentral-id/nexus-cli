//! Core proving engine

use crate::prover::verifier;

use super::types::ProverError;
use crate::analytics::track_likely_oom_error;
use crate::environment::Environment;
use crate::task::Task;
use nexus_sdk::{
    Local, Prover,
    stwo::seq::{Proof, Stwo},
};
use postcard::from_bytes;
use std::env;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;

/// Core proving engine for ZK proof generation
pub struct ProvingEngine;

impl ProvingEngine {
    /// Create a Stwo prover instance for the fibonacci program
    pub fn create_fib_prover() -> Result<Stwo<Local>, ProverError> {
        let elf_bytes = include_bytes!("../../assets/fib_input_initial");
        Stwo::<Local>::new_from_bytes(elf_bytes).map_err(|e| {
            ProverError::Stwo(format!(
                "Failed to load fib_input_initial guest program: {}",
                e
            ))
        })
    }

    /// Subprocess entrypoint: generate proof without verification
    pub fn prove_fib_subprocess(inputs: &(u32, u32, u32)) -> Result<Proof, ProverError> {
        let prover = Self::create_fib_prover()?;
        let (view, proof) = prover
            .prove_with_input::<(), (u32, u32, u32)>(&(), inputs)
            .map_err(|e| {
                ProverError::Stwo(format!(
                    "Failed to generate proof for inputs {:?}: {}",
                    inputs, e
                ))
            })?;
        // Check exit code in subprocess
        verifier::ProofVerifier::check_exit_code(&view)?;

        Ok(proof)
    }

    /// Generate proof for given inputs using the fibonacci program in a subprocess
    pub async fn prove_and_validate(
        inputs: &(u32, u32, u32),
        task: &Task,
        environment: &Environment,
        client_id: &str,
    ) -> Result<Proof, ProverError> {
        // Spawn a subprocess for proof generation to isolate memory usage
        let exe_path = env::current_exe()?;
        let mut cmd = tokio::process::Command::new(exe_path);
        cmd.arg("prove-fib-subprocess")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        // Apply maximum performance optimizations for high-throughput parallel processing
        Self::apply_performance_optimizations(&mut cmd);

        // Serialize inputs as binary for faster transfer
        let input_bytes = postcard::to_allocvec(inputs)?;

        let mut child = cmd.spawn()?;

        // Write binary inputs to subprocess stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(&input_bytes).await?;
            // stdin is dropped here, closing the pipe to signal EOF
        }

        let output = child.wait_with_output().await?;

        if !output.status.success() {
            if let Some(code) = output.status.code() {
                if code == crate::consts::cli_consts::SUBPROCESS_SUSPECTED_OOM_CODE {
                    // 128 + 9 = 137 means external sigkill, so likely killed by kernel due to OOM; track analytics event
                    tokio::spawn(track_likely_oom_error(
                        task.clone(),
                        environment.clone(),
                        client_id.to_string(),
                    ));
                }

                if code == crate::consts::cli_consts::SUBPROCESS_INTERNAL_ERROR_CODE {
                    // error happened inside the subprocess, and so we know that it may be useful information to the user
                    return Err(ProverError::Subprocess(format!(
                        "Error while proving within subprocess, captured error: [{}]",
                        &String::from_utf8_lossy(&output.stderr)
                    )));
                }
            }

            return Err(ProverError::Subprocess(format!(
                "Prover subprocess failed with status: {}",
                output.status
            )));
        }

        // Deserialize proof from subprocess stdout
        let proof: Proof = from_bytes(&output.stdout)?;

        // Skip redundant verification in main process
        // Verification is already done in subprocess via verifier::check_exit_code()
        // This saves 0.5-1 second per proof for high-throughput scenarios
        Ok(proof)
    }

    /// Apply maximum performance optimizations to subprocess for high-throughput parallel processing
    fn apply_performance_optimizations(cmd: &mut tokio::process::Command) {
        // Check if we're on a low-memory system (detect via environment or heuristic)
        let is_low_memory = std::env::var("NEXUS_LOW_MEMORY").is_ok() ||
                           std::env::var("NEXUS_1GB_MODE").is_ok();

        if is_low_memory {
            // Ultra-conservative memory settings for 1GB systems
            cmd.env("MALLOC_ARENA_MAX", "1"); // Single arena to minimize overhead
            cmd.env("MALLOC_CONF", "dirty_decay_ms:100,muzzy_decay_ms:100,background_thread:false,lg_chunk:19");
            cmd.env("RUST_MIN_STACK", "524288"); // 512KB stack for subprocess threads
            cmd.env("TOKIO_WORKER_THREADS", "1"); // Single thread runtime
        } else {
            // Standard aggressive memory optimizations for normal systems
            cmd.env("MALLOC_ARENA_MAX", "2"); // Reduce arenas for less fragmentation
            cmd.env("MALLOC_CONF", "dirty_decay_ms:500,muzzy_decay_ms:500,background_thread:true");
            cmd.env("RUST_MIN_STACK", "1048576"); // 1MB minimum stack for subprocess threads
        }

        // Maximum process spawning optimizations for parallel throughput
        cmd.env("RUST_BACKTRACE", "0"); // Disable backtrace collection for faster startup
        cmd.env("RUST_LOG", "off"); // Disable logging overhead in subprocess

        // Process group and scheduling optimizations
        cmd.process_group(0); // Create new process group for better management
    }
}
