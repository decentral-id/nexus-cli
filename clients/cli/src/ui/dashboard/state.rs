//! Dashboard state management
//!
//! Contains the main dashboard state struct and related enums

use crate::consts::cli_consts::MAX_ACTIVITY_LOGS;
use crate::environment::Environment;
use crate::events::{Event as WorkerEvent, ProverState};
use crate::ui::app::UIConfig;
use crate::ui::metrics::{SystemMetrics, TaskFetchInfo, ZkVMMetrics};

use std::collections::VecDeque;
use std::time::Instant;
use sysinfo::System;

/// State for tracking fetching operations
#[derive(Debug, Clone)]
pub enum FetchingState {
    Idle,
    Active { started_at: Instant },
    Timeout,
}

/// Enhanced dashboard state with real-time metrics and animations.
#[derive(Debug)]
pub struct DashboardState {
    /// Unique identifier for the node.
    pub node_id: Option<u64>,
    /// The environment in which the application is running.
    pub environment: Environment,
    /// The start time of the application, used for computing uptime.
    pub start_time: Instant,
    /// Last task fetched ID
    pub last_task: Option<String>,
    /// The current task being executed by the node, if any.
    pub current_task: Option<String>,
    /// Total RAM available on the machine, in GB.
    pub total_ram_gb: f64,
    /// Number of worker threads being used for proving.
    pub num_threads: usize,
    /// Queue of events waiting to be processed
    pub pending_events: VecDeque<WorkerEvent>,
    /// Activity logs for display (last 50 events)
    pub activity_logs: VecDeque<WorkerEvent>,
    /// Whether a new version is available.
    pub update_available: bool,
    /// The latest version string, if known.
    pub latest_version: Option<String>,
    /// Whether to enable background colors
    pub with_background_color: bool,

    /// System metrics (CPU, RAM, etc.)
    pub system_metrics: SystemMetrics,
    /// zkVM task metrics
    pub zkvm_metrics: ZkVMMetrics,
    /// Task fetch information for accurate timing
    pub task_fetch_info: TaskFetchInfo,
    /// Animation tick counter
    pub tick: usize,

    /// Timestamp of last successful proof submission
    last_submission_timestamp: Option<String>,
    /// Current fetching state (active, timeout, idle)
    fetching_state: FetchingState,
    /// Persistent system info instance for accurate CPU measurements
    sysinfo: System,
    /// Current prover state from state events
    current_prover_state: ProverState,
    /// Track when Step 2 started for current task
    pub step2_start_time: Option<Instant>,
    /// Track the start time and original wait duration for current waiting period
    pub waiting_start_info: Option<(Instant, u64)>, // (start_time, original_wait_secs)
}

impl DashboardState {
    /// Creates a new instance of the dashboard state.
    pub fn new(
        node_id: Option<u64>,
        environment: Environment,
        start_time: Instant,
        ui_config: UIConfig,
    ) -> Self {
        // Check if we're on a low-memory system
        let is_low_memory = std::env::var("NEXUS_1GB_MODE").is_ok();

        // Reduce buffer sizes on low-memory systems
        let max_activity_logs = if is_low_memory { 25 } else { MAX_ACTIVITY_LOGS };

        Self {
            node_id,
            environment,
            start_time,
            last_task: None,
            current_task: None,
            total_ram_gb: crate::system::total_memory_gb(),
            num_threads: ui_config.num_threads,
            pending_events: VecDeque::with_capacity(if is_low_memory { 50 } else { 100 }),
            activity_logs: VecDeque::with_capacity(max_activity_logs),
            update_available: ui_config.update_available,
            latest_version: ui_config.latest_version,
            with_background_color: ui_config.with_background_color,

            system_metrics: SystemMetrics::default(),
            zkvm_metrics: ZkVMMetrics::default(),
            task_fetch_info: TaskFetchInfo::default(),
            tick: 0,
            last_submission_timestamp: None,
            fetching_state: FetchingState::Idle,
            sysinfo: if is_low_memory {
                System::new_with_specifics(sysinfo::RefreshKind::nothing())
            } else {
                System::new_all() // Initialize with all data for first refresh
            },
            current_prover_state: ProverState::Waiting,
            step2_start_time: None,
            waiting_start_info: None,
        }
    }
    // Getter methods for private fields
    pub fn fetching_state(&self) -> &FetchingState {
        &self.fetching_state
    }

    pub fn last_submission_timestamp(&self) -> &Option<String> {
        &self.last_submission_timestamp
    }

    // Setter methods for private fields (for updaters)
    pub fn set_fetching_state(&mut self, state: FetchingState) {
        self.fetching_state = state;
    }

    pub fn current_prover_state(&self) -> ProverState {
        self.current_prover_state
    }

    pub fn set_current_prover_state(&mut self, state: ProverState) {
        self.current_prover_state = state;
    }

    pub fn set_last_submission_timestamp(&mut self, timestamp: Option<String>) {
        self.last_submission_timestamp = timestamp;
    }

    pub fn get_sysinfo_mut(&mut self) -> &mut System {
        &mut self.sysinfo
    }

    /// Add an event to activity logs with size limit
    pub fn add_to_activity_log(&mut self, event: WorkerEvent) {
        let is_low_memory = std::env::var("NEXUS_1GB_MODE").is_ok();
        let max_logs = if is_low_memory { 25 } else { MAX_ACTIVITY_LOGS };

        if self.activity_logs.len() >= max_logs {
            self.activity_logs.pop_front();
        }
        self.activity_logs.push_back(event);
    }

    /// Add an event to the processing queue
    pub fn add_event(&mut self, event: WorkerEvent) {
        self.pending_events.push_back(event);
    }
}
