//! Proof verification

use super::types::ProverError;
use nexus_sdk::{KnownExitCodes, Viewable};

/// Proof verifier for validating generated proofs
pub struct ProofVerifier;

impl ProofVerifier {
    /// Check exit code from proof execution
    pub fn check_exit_code<T: Viewable>(view: &T) -> Result<(), ProverError> {
        let exit_code = view.exit_code().map_err(|e| {
            ProverError::GuestProgram(format!("Failed to deserialize exit code: {}", e))
        })?;

        if exit_code != KnownExitCodes::ExitSuccess as u32 {
            return Err(ProverError::GuestProgram(format!(
                "Prover exited with non-zero exit code: {}",
                exit_code
            )));
        }

        Ok(())
    }
}
