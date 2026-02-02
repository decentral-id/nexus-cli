# Low-Resource Deployment Guide

## Running Nexus CLI on 1 CPU Core / 1 GB RAM Systems

This guide explains how to run the Nexus CLI prover on severely resource-constrained systems.

## Prerequisites

- **Build Environment**: Standard PC with ≥4GB RAM (for compilation)
- **Target Environment**: Low-end PC with 1 CPU core and 1 GB RAM
- **OS**: Linux (tested on Ubuntu/Debian)

## Building the Binary

Build on a standard PC with adequate resources:

```bash
# On your development/build machine
cd /home/cody/nexus-cli/clients/cli

# Build optimized release binary
cargo build --release

# Binary will be at: target/release/nexus-network
```

## Deploying to Low-Resource System

Copy the binary to your target system:

```bash
# From build machine
scp target/release/nexus-network user@low-resource-host:/usr/local/bin/

# Or via USB drive, network share, etc.
```

## Recommended Runtime Configuration

### Command-Line Flags

For 1GB RAM systems, **always** use these flags:

```bash
nexus-network start \
  --headless \
  --max-threads 1 \
  --max-difficulty SMALL \
  --check-memory
```

**Flag Explanations**:

- `--headless`: Runs without TUI (saves ~50-100MB RAM)
- `--max-threads 1`: Limits to single-threaded operation
- `--max-difficulty SMALL`: Prevents attempting large proofs that cause OOM
- `--check-memory`: Enables memory monitoring (recommended)

### Optional Flags

```bash
--max-tasks 100        # Auto-exit after 100 proofs (prevents memory leaks accumulation)
--orchestrator-url ... # Custom orchestrator if needed
```

### Environment Variables

Set these before running:

```bash
export TOKIO_WORKER_THREADS=1       # Limit async runtime threads
export RUST_MIN_STACK=2097152       # 2MB stack (vs default 8MB)
export RUST_BACKTRACE=0             # Disable backtraces to save memory
```

## Complete Startup Script

Create `/usr/local/bin/nexus-low-mem.sh`:

```bash
#!/bin/bash

# Environment configuration
export TOKIO_WORKER_THREADS=1
export RUST_MIN_STACK=2097152
export RUST_BACKTRACE=0

# Check available memory
AVAILABLE_RAM=$(free -g | awk '/^Mem:/{print $7}')
if [ "$AVAILABLE_RAM" -lt 1 ]; then
    echo "ERROR: Less than 1GB free RAM available"
    exit 1
fi

# Run prover
/usr/local/bin/nexus-network start \
    --headless \
    --max-threads 1 \
    --max-difficulty SMALL \
    --check-memory \
    --max-tasks 100

echo "Prover exited. Exit code: $?"
```

Make executable:

```bash
chmod +x /usr/local/bin/nexus-low-mem.sh
```

## System Optimization

### 1. Enable Swap (Recommended)

For systems with exactly 1GB RAM, adding swap helps prevent OOM kills:

```bash
# Create 2GB swap file
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Make permanent
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Verify
free -h
```

**Note**: Swap will be slower but prevents crashes.

### 2. Disable Unnecessary Services

Free up RAM by stopping unused services:

```bash
# Example: stop GUI if running headless
sudo systemctl stop gdm  # or lightdm, sddm, etc.

# Disable other services
sudo systemctl disable bluetooth
sudo systemctl disable cups  # printing
```

### 3. Set Memory Limits (Optional)

Use cgroups to hard-limit memory usage:

```bash
# Create cgroup
sudo cgcreate -g memory:/nexus

# Set 900MB limit (leaving 100MB for system)
sudo cgset -r memory.limit_in_bytes=943718400 nexus

# Run in cgroup
sudo cgexec -g memory:nexus /usr/local/bin/nexus-low-mem.sh
```

## Monitoring

### Real-Time Memory Monitoring

In another terminal, monitor memory usage:

```bash
# Watch memory every 1 second
watch -n 1 free -h

# Or monitor specific process
watch -n 1 'ps aux | grep nexus-network | grep -v grep'
```

### Log Memory Status

The optimized CLI will log memory-related events:

```
Initializing process pool with 1 max processes (1.0GB RAM detected)
Low memory detected (1.0GB) - skipping pool pre-warming to conserve RAM
Ultra-low memory detected (1.0GB) - disabling batching (batch size = 1)
```

## Performance Expectations

With 1 CPU core and 1 GB RAM:

| Metric | Expected Value |
|--------|----------------|
| **Memory Usage** | 200-400 MB (idle to active) |
| **Proof Generation Speed** | 5-10x slower than multi-core |
| **Concurrent Proofs** | 1 (no parallelization) |
| **Task Difficulty** | SMALL only (larger tasks may OOM) |
| **Throughput** | ~1 proof per 10-30 seconds (varies by complexity) |

## Troubleshooting

### Out-of-Memory (OOM) Kills

**Symptoms**: Process suddenly disappears

```bash
# Check system logs
dmesg | grep -i "killed process"
# or
journalctl -xe | grep -i oom
```

**Solutions**:

1. Enable swap (see above)
2. Use `--max-difficulty SMALL` (if not already)
3. Reduce `--max-tasks` to restart more frequently
4. Check for memory leaks: `smem -k | grep nexus`

### Slow Performance

**Expected**: 5-10x slower is normal for single-core systems.

**If unusually slow** (>30s per SMALL task):

1. Check CPU throttling: `cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`
2. Enable performance mode: `sudo cpupower frequency-set -g performance`
3. Check system load: `uptime` (load should be <2.0 for single core)

### Memory Warnings Not Appearing

If you don't see memory warnings on a 1GB system:

```bash
# Verify memory detection  
free -g

# Check if CLI sees correct amount
# (Look for "X.XGB RAM detected" in output)
```

## Docker Alternative

If running in Docker, enforce hard limits:

```dockerfile
FROM debian:bookworm-slim

COPY target/release/nexus-network /usr/local/bin/

ENV TOKIO_WORKER_THREADS=1
ENV RUST_MIN_STACK=2097152

CMD ["nexus-network", "start", "--headless", "--max-threads", "1", "--max-difficulty", "SMALL"]
```

Run with limits:

```bash
docker run \
  --memory=1g \
  --memory-swap=1g \
  --cpus=1 \
  --name nexus-prover \
  nexus-low-mem
```

## Advanced: systemd Service

Create `/etc/systemd/system/nexus-prover.service`:

```ini
[Unit]
Description=Nexus Network Prover (Low-Memory Mode)
After=network.target

[Service]
Type=simple
User=nexus
Group=nexus
Environment="TOKIO_WORKER_THREADS=1"
Environment="RUST_MIN_STACK=2097152"
Environment="RUST_BACKTRACE=0"
ExecStart=/usr/local/bin/nexus-network start --headless --max-threads 1 --max-difficulty SMALL --max-tasks 100
Restart=always
RestartSec=10

# Memory limits (requires systemd 230+)
MemoryMax=900M
MemoryHigh=800M

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable nexus-prover
sudo systemctl start nexus-prover

# Check status
sudo systemctl status nexus-prover

# View logs
sudo journalctl -u nexus-prover -f
```

## Summary

✅ **Key Takeaways**:

- Build on standard PC, deploy binary to low-resource system
- **Always use**: `--headless --max-threads 1 --max-difficulty SMALL`
- Enable swap for stability (2GB recommended)
- Expect 5-10x slower performance
- Monitor memory with `watch -n 1 free -h`
- Memory footprint: 200-400 MB

⚠️ **Important Limitations**:

- Only SMALL difficulty tasks recommended
- No parallelization (1 proof at a time)
- OOM kills possible under heavy load
- TUI mode not recommended (use headless)
