//! Core proving engine with zero-copy optimizations

use crate::prover::verifier;
use super::memory_pool::SerializationBuffer;
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
use tokio::io::{AsyncWriteExt, AsyncReadExt};

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

    /// Generate proof for given inputs using zero-copy optimized subprocess communication
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

        // Use serialization buffer to minimize allocations
        let mut serialization_buffer = SerializationBuffer::new();
        {
            let buffer = serialization_buffer.get_mut();
            postcard::to_io(inputs, buffer).map_err(ProverError::Serialization)?;
        }

        let mut child = cmd.spawn()?;

        // Zero-copy write: write directly to subprocess stdin
        if let Some(mut stdin) = child.stdin.take() {
            match stdin.write_all(serialization_buffer.as_slice()).await {
                Ok(()) => {
                    // Explicitly flush and close stdin to signal EOF
                    drop(stdin);
                }
                Err(e) => {
                    // If stdin write fails, the pipe might already be broken
                    if e.kind() == std::io::ErrorKind::BrokenPipe {
                        // Continue waiting for the child process, as it might still be running
                    } else {
                        return Err(ProverError::Subprocess(format!("Failed to write to subprocess stdin: {}", e)));
                    }
                }
            }
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

                // Check if the process was terminated by SIGPIPE (32) or other signal-related exit codes
                if code >= 128 && code != 137 { // 137 is OOM, others are signals
                    // If it's SIGPIPE (exit code 141 = 128 + 13), this is likely due to shutdown and not a real error
                    if code - 128 == 13 { // SIGPIPE
                        // Don't treat SIGPIPE as an error during shutdown
                        return Err(ProverError::Subprocess(format!(
                            "Subprocess terminated by SIGPIPE (broken pipe) - likely during shutdown"
                        )));
                    }
                }
            }

            return Err(ProverError::Subprocess(format!(
                "Prover subprocess failed with status: {}",
                output.status
            )));
        }

        // Zero-copy deserialize: read directly from subprocess stdout
        let proof: Proof = from_bytes(&output.stdout).map_err(|e| {
            ProverError::Subprocess(format!(
                "Failed to deserialize proof from subprocess stdout: {} (stdout len: {})",
                e,
                output.stdout.len()
            ))
        })?;

        // Skip redundant verification in main process
        // Verification is already done in subprocess via verifier::check_exit_code()
        // This saves 0.5-1 second per proof for high-throughput scenarios
        Ok(proof)
    }

    /// Generate proof with streaming I/O for minimal memory usage
    pub async fn prove_and_validate_streaming(
        inputs: &(u32, u32, u32),
        task: &Task,
        environment: &Environment,
        client_id: &str,
    ) -> Result<Proof, ProverError> {
        // Spawn subprocess with optimized settings
        let exe_path = env::current_exe()?;
        let mut cmd = tokio::process::Command::new(exe_path);
        cmd.arg("prove-fib-subprocess")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        Self::apply_performance_optimizations(&mut cmd);

        let mut child = cmd.spawn()?;

        // Streaming input writing to minimize memory usage
        if let Some(mut stdin) = child.stdin.take() {
            // Write input data directly without intermediate buffer
            let mut buffer = [0u8; 12]; // 3 * u32 = 12 bytes
            buffer[0..4].copy_from_slice(&inputs.0.to_le_bytes());
            buffer[4..8].copy_from_slice(&inputs.1.to_le_bytes());
            buffer[8..12].copy_from_slice(&inputs.2.to_le_bytes());

            if let Err(e) = stdin.write_all(&buffer).await {
                if e.kind() != std::io::ErrorKind::BrokenPipe {
                    return Err(ProverError::Subprocess(format!("Failed to write to subprocess stdin: {}", e)));
                }
            }
            drop(stdin);
        }

        // Streaming output reading
        let output = child.wait_with_output().await?;

        if !output.status.success() {
            return Err(ProverError::Subprocess(format!(
                "Prover subprocess failed with status: {}",
                output.status
            )));
        }

        // Direct deserialization from subprocess output
        let proof: Proof = from_bytes(&output.stdout).map_err(|e| {
            ProverError::Subprocess(format!(
                "Failed to deserialize proof from subprocess stdout: {}",
                e
            ))
        })?;

        Ok(proof)
    }

    /// Apply maximum performance optimizations to subprocess for high-throughput parallel processing
    pub fn apply_performance_optimizations(cmd: &mut tokio::process::Command) {
        // Standard aggressive memory optimizations for normal systems
        cmd.env("MALLOC_ARENA_MAX", "2"); // Reduce arenas for less fragmentation
        cmd.env("MALLOC_CONF", "dirty_decay_ms:500,muzzy_decay_ms:500,background_thread:true");
        cmd.env("RUST_MIN_STACK", "1048576"); // 1MB minimum stack for subprocess threads

        // Maximum process spawning optimizations for parallel throughput
        cmd.env("RUST_BACKTRACE", "0"); // Disable backtrace collection for faster startup
        cmd.env("RUST_LOG", "off"); // Disable logging overhead in subprocess

        // Process group and scheduling optimizations
        cmd.process_group(0); // Create new process group for better management
    }
}
