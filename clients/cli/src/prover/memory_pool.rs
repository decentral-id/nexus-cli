//! Memory pool allocation system for high-performance proof generation

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use nexus_sdk::stwo::seq::Proof;

/// Thread-safe memory pool for reusable buffers
pub struct MemoryPool<T> {
    pool: Arc<Mutex<VecDeque<T>>>,
    max_size: usize,
}

impl<T> MemoryPool<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
        }
    }

    pub fn acquire(&self) -> Option<T> {
        let mut pool = self.pool.lock().unwrap();
        pool.pop_front()
    }

    pub fn release(&self, item: T) {
        let mut pool = self.pool.lock().unwrap();
        if pool.len() < self.max_size {
            pool.push_back(item);
        }
    }
}

/// Arena allocator for batch operations
pub struct BatchArena {
    buffers: Vec<Vec<u8>>,
    proofs: Vec<Proof>,
}

impl BatchArena {
    pub fn new() -> Self {
        Self {
            buffers: Vec::new(),
            proofs: Vec::new(),
        }
    }

    pub fn get_buffer(&mut self) -> &mut Vec<u8> {
        self.buffers.push(Vec::new());
        self.buffers.last_mut().unwrap()
    }

    pub fn store_proof(&mut self, proof: Proof) {
        self.proofs.push(proof);
    }

    pub fn take_proofs(&mut self) -> Vec<Proof> {
        std::mem::take(&mut self.proofs)
    }
}

impl Default for BatchArena {
    fn default() -> Self {
        Self::new()
    }
}

/// Reusable serialization buffer to minimize allocations
pub struct SerializationBuffer {
    buffer: Vec<u8>,
}

impl SerializationBuffer {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(4096), // Pre-allocate 4KB
        }
    }

    pub fn get_mut(&mut self) -> &mut Vec<u8> {
        self.buffer.clear();
        &mut self.buffer
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }
}

impl Default for SerializationBuffer {
    fn default() -> Self {
        Self::new()
    }
}