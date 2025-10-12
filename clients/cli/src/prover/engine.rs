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
use serde_json;
use std::env;
use std::process::Stdio;

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
            .arg("--inputs")
            .arg(serde_json::to_string(inputs)?)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        // Apply system-level performance optimizations
        Self::apply_performance_optimizations(&mut cmd);

        let output = cmd.output().await?;

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

        // Verify proof in main process
        let verify_prover = Self::create_fib_prover()?;
        verifier::ProofVerifier::verify_proof(&proof, inputs, &verify_prover)?;

        Ok(proof)
    }

    /// Apply system-level performance optimizations to subprocess
    fn apply_performance_optimizations(cmd: &mut tokio::process::Command) {
        let cores = crate::system::num_cores();
        let threads_param = cores.to_string();

        // High priority process scheduling (lower nice number = higher priority)
        cmd.env("RENEICE", "-n -10 $$");

        // CPU affinity to all available cores
        let cpu_mask = (0..cores).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        cmd.env("TASKSET", format!("--cpu-list {}", cpu_mask));

        // Real-time I/O priority (class 1 = real-time, priority 0 = highest)
        cmd.env("IONICE", "-c 1 -n 0");

        // Performance CPU governor
        cmd.env("CPU_GOVERNOR", "performance");

        // Parallel processing library optimization
        cmd.env("RAYON_NUM_THREADS", &threads_param);
        cmd.env("TOKIO_WORKER_THREADS", &threads_param);
        cmd.env("OMP_NUM_THREADS", &threads_param);
        cmd.env("MKL_NUM_THREADS", &threads_param);
        cmd.env("VECLIB_MAXIMUM_THREADS", &threads_param);

        // Memory and NUMA optimizations
        cmd.env("NUMA_POLICY", "interleave");
        cmd.env("MALLOC_ARENA_MAX", "4");
        cmd.env("MALLOC_MMAP_THRESHOLD_", "16384");
    }
}
