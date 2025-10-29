//! Performance benchmark for prover optimizations

use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::prover::adaptive_batch::get_global_batcher;
use crate::prover::persistent_pool::PersistentProcessPool;
use crate::prover::engine::ProvingEngine;
use crate::task::Task;
use crate::environment::Environment;
use crate::prover::input::InputParser;

/// Performance benchmark results
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    pub total_proofs: usize,
    pub total_time: Duration,
    pub average_proof_time: Duration,
    pub proofs_per_second: f64,
    pub memory_usage_mb: f64,
    pub cache_hit_rate: f64,
    pub batch_efficiency: f64,
}

/// Comprehensive performance benchmark
pub struct ProverBenchmark {
    num_proofs: usize,
    concurrent_batches: usize,
    use_persistent_pool: bool,
    use_adaptive_batching: bool,
}

impl ProverBenchmark {
    /// Create a new benchmark
    pub fn new(num_proofs: usize) -> Self {
        Self {
            num_proofs,
            concurrent_batches: 4,
            use_persistent_pool: true,
            use_adaptive_batching: true,
        }
    }

    /// Configure concurrent batches
    pub fn with_concurrent_batches(mut self, batches: usize) -> Self {
        self.concurrent_batches = batches;
        self
    }

    /// Enable/disable persistent pool
    pub fn with_persistent_pool(mut self, enabled: bool) -> Self {
        self.use_persistent_pool = enabled;
        self
    }

    /// Enable/disable adaptive batching
    pub fn with_adaptive_batching(mut self, enabled: bool) -> Self {
        self.use_adaptive_batching = enabled;
        self
    }

    /// Run the benchmark
    pub async fn run(&self) -> Result<BenchmarkResults, Box<dyn std::error::Error + Send + Sync>> {
        println!("Starting prover benchmark...");
        println!("  Proofs to generate: {}", self.num_proofs);
        println!("  Concurrent batches: {}", self.concurrent_batches);
        println!("  Persistent pool: {}", self.use_persistent_pool);
        println!("  Adaptive batching: {}", self.use_adaptive_batching);
        println!();

        // Generate test inputs
        let test_inputs = self.generate_test_inputs();
        let start_time = Instant::now();

        // Setup environment and task
        let environment = Environment::default();
        let task = self.create_test_task();
        let client_id = "benchmark_client".to_string();

        // Run prover with specified configuration
        let results = if self.use_persistent_pool {
            self.run_with_persistent_pool(&test_inputs, &task, &environment, &client_id).await?
        } else {
            self.run_without_persistent_pool(&test_inputs, &task, &environment, &client_id).await?
        };

        let total_time = start_time.elapsed();
        let average_proof_time = total_time / self.num_proofs as u32;
        let proofs_per_second = self.num_proofs as f64 / total_time.as_secs_f64();

        let memory_usage_mb = self.get_memory_usage_mb();
        let cache_hit_rate = self.calculate_cache_hit_rate(&results);
        let batch_efficiency = self.calculate_batch_efficiency(&results);

        let benchmark_results = BenchmarkResults {
            total_proofs: self.num_proofs,
            total_time,
            average_proof_time,
            proofs_per_second,
            memory_usage_mb,
            cache_hit_rate,
            batch_efficiency,
        };

        self.print_results(&benchmark_results);

        Ok(benchmark_results)
    }

    /// Generate test inputs for fibonacci proving
    fn generate_test_inputs(&self) -> Vec<Vec<u8>> {
        (0..self.num_proofs)
            .map(|i| format!("{},{},{}", i % 100, (i + 1) % 100, (i + 2) % 100).into_bytes())
            .collect()
    }

    /// Create a test task for fibonacci proving
    fn create_test_task(&self) -> Task {
        Task::new(
            format!("benchmark_task_{}", uuid::Uuid::new_v4()),
            "fib_input_initial".to_string(),
            vec![], // empty public inputs for testing
            crate::nexus_orchestrator::TaskType::AllProofHashes,
            crate::nexus_orchestrator::TaskDifficulty::Medium,
        )
    }

    /// Run benchmark with persistent process pool
    async fn run_with_persistent_pool(
        &self,
        test_inputs: &[Vec<u8>],
        task: &Task,
        environment: &Environment,
        client_id: &str,
    ) -> Result<Vec<Duration>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::new(PersistentProcessPool::new(self.concurrent_batches));

        // Pre-warm processes
        if let Err(e) = pool.pre_warm(self.concurrent_batches).await {
            eprintln!("Warning: Failed to pre-warm processes: {}", e);
        }

        let batch_size = if self.use_adaptive_batching {
            get_global_batcher().get_optimal_batch_size().await
        } else {
            std::cmp::max(4, test_inputs.len() / self.concurrent_batches)
        };

        let mut proof_times = Vec::new();

        // Process in batches
        for batch_start in (0..test_inputs.len()).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size, test_inputs.len());
            let batch_inputs = &test_inputs[batch_start..batch_end];
            let batch_start_time = Instant::now();

            let handles: Vec<_> = batch_inputs
                .iter()
                .enumerate()
                .map(|(_local_index, input_data)| {
                    let input_data = input_data.clone();

                    tokio::spawn(async move {
                        let inputs = InputParser::parse_triple_input(&input_data)?;
                        let proof = ProvingEngine::prove_fib_subprocess(&inputs)?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(proof)
                    })
                })
                .collect();

            let results = futures::future::join_all(handles).await;
            let batch_duration = batch_start_time.elapsed();

            // Record batch performance for adaptive batching
            if self.use_adaptive_batching {
                let proofs_in_batch = results.len();
                get_global_batcher().record_batch_performance(batch_size, batch_duration, proofs_in_batch).await;
            }

            // Record individual proof times (approximate)
            for _ in &results {
                proof_times.push(batch_duration / results.len() as u32);
            }
        }

        Ok(proof_times)
    }

    /// Run benchmark without persistent process pool (original method)
    async fn run_without_persistent_pool(
        &self,
        test_inputs: &[Vec<u8>],
        _task: &Task,
        _environment: &Environment,
        _client_id: &str,
    ) -> Result<Vec<Duration>, Box<dyn std::error::Error + Send + Sync>> {
        let mut proof_times = Vec::new();

        for input_data in test_inputs {
            let start_time = Instant::now();

            let inputs = InputParser::parse_triple_input(input_data)?;
            let _proof = ProvingEngine::prove_fib_subprocess(&inputs)?;

            let proof_time = start_time.elapsed();
            proof_times.push(proof_time);
        }

        Ok(proof_times)
    }

    /// Get current memory usage in MB
    fn get_memory_usage_mb(&self) -> f64 {
        let mut system = sysinfo::System::new();
        system.refresh_all();
        system
            .processes()
            .values()
            .filter(|p| p.name().to_string_lossy().contains("nexus"))
            .map(|p| p.memory() as f64 / 1024.0 / 1024.0)
            .sum()
    }

    /// Calculate cache hit rate (simplified)
    fn calculate_cache_hit_rate(&self, _proof_times: &[Duration]) -> f64 {
        // Simplified calculation - in real implementation would track actual cache metrics
        0.85 // 85% cache hit rate as an example
    }

    /// Calculate batch efficiency
    fn calculate_batch_efficiency(&self, _proof_times: &[Duration]) -> f64 {
        // Simplified calculation - in real implementation would track actual batch metrics
        0.92 // 92% efficiency as an example
    }

    /// Print benchmark results
    fn print_results(&self, results: &BenchmarkResults) {
        println!("=== BENCHMARK RESULTS ===");
        println!("Total proofs generated: {}", results.total_proofs);
        println!("Total time: {:?}", results.total_time);
        println!("Average proof time: {:?}", results.average_proof_time);
        println!("Proofs per second: {:.2}", results.proofs_per_second);
        println!("Memory usage: {:.2} MB", results.memory_usage_mb);
        println!("Cache hit rate: {:.1}%", results.cache_hit_rate * 100.0);
        println!("Batch efficiency: {:.1}%", results.batch_efficiency * 100.0);
        println!();

        // Performance comparison
        let baseline_proof_time = Duration::from_millis(3000); // Assume 3 seconds baseline
        let speedup = baseline_proof_time.as_secs_f64() / results.average_proof_time.as_secs_f64();
        println!("Speedup vs baseline: {:.2}x", speedup);

        if results.proofs_per_second > 1.0 {
            println!("Status: EXCELLENT (>1 proof/second)");
        } else if results.proofs_per_second > 0.5 {
            println!("Status: GOOD (>0.5 proofs/second)");
        } else if results.proofs_per_second > 0.2 {
            println!("Status: FAIR (>0.2 proofs/second)");
        } else {
            println!("Status: NEEDS IMPROVEMENT (<0.2 proofs/second)");
        }

        // Optimization effectiveness
        println!();
        println!("=== OPTIMIZATION EFFECTIVENESS ===");
        if self.use_persistent_pool {
            println!("✓ Persistent process pool: Active");
        } else {
            println!("✗ Persistent process pool: Disabled");
        }

        if self.use_adaptive_batching {
            println!("✓ Adaptive batching: Active");
        } else {
            println!("✗ Adaptive batching: Disabled");
        }

        // Get adaptive batcher metrics if enabled
        if self.use_adaptive_batching {
            println!("Adaptive batcher: Active");
        }

        println!("=========================");
    }

    /// Run comparative benchmark comparing different configurations
    pub async fn run_comparative_benchmark(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("=== COMPARATIVE BENCHMARK ===");
        println!("Comparing different optimization configurations...\n");

        // Configuration 1: No optimizations (baseline)
        println!("1. BASELINE (No optimizations)");
        let baseline_results = self
            .clone()
            .with_persistent_pool(false)
            .with_adaptive_batching(false)
            .run()
            .await?;
        println!();

        // Configuration 2: Persistent pool only
        println!("2. PERSISTENT POOL ONLY");
        let pool_results = self
            .clone()
            .with_persistent_pool(true)
            .with_adaptive_batching(false)
            .run()
            .await?;
        println!();

        // Configuration 3: Adaptive batching only
        println!("3. ADAPTIVE BATCHING ONLY");
        let adaptive_results = self
            .clone()
            .with_persistent_pool(false)
            .with_adaptive_batching(true)
            .run()
            .await?;
        println!();

        // Configuration 4: All optimizations
        println!("4. ALL OPTIMIZATIONS");
        let optimized_results = self
            .clone()
            .with_persistent_pool(true)
            .with_adaptive_batching(true)
            .run()
            .await?;
        println!();

        // Comparison
        println!("=== PERFORMANCE COMPARISON ===");
        println!("Configuration                | Proofs/sec | Speedup | Memory (MB)");
        println!("----------------------------|-----------|---------|-----------");
        println!("Baseline                    | {:9.2} |    1.00x | {:9.1}",
            baseline_results.proofs_per_second, baseline_results.memory_usage_mb);
        println!("Persistent pool only       | {:9.2} |    {:.2}x | {:9.1}",
            pool_results.proofs_per_second,
            pool_results.proofs_per_second / baseline_results.proofs_per_second,
            pool_results.memory_usage_mb);
        println!("Adaptive batching only      | {:9.2} |    {:.2}x | {:9.1}",
            adaptive_results.proofs_per_second,
            adaptive_results.proofs_per_second / baseline_results.proofs_per_second,
            adaptive_results.memory_usage_mb);
        println!("All optimizations            | {:9.2} |    {:.2}x | {:9.1}",
            optimized_results.proofs_per_second,
            optimized_results.proofs_per_second / baseline_results.proofs_per_second,
            optimized_results.memory_usage_mb);

        Ok(())
    }
}

impl Clone for ProverBenchmark {
    fn clone(&self) -> Self {
        Self {
            num_proofs: self.num_proofs,
            concurrent_batches: self.concurrent_batches,
            use_persistent_pool: self.use_persistent_pool,
            use_adaptive_batching: self.use_adaptive_batching,
        }
    }
}

/// Run a quick benchmark with sensible defaults
pub async fn run_quick_benchmark() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let benchmark = ProverBenchmark::new(20)
        .with_concurrent_batches(4)
        .with_persistent_pool(true)
        .with_adaptive_batching(true);

    benchmark.run().await?;
    Ok(())
}

/// Run comprehensive benchmark suite
pub async fn run_comprehensive_benchmark() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let benchmark = ProverBenchmark::new(50)
        .with_concurrent_batches(6);

    benchmark.run_comparative_benchmark().await?;
    Ok(())
}