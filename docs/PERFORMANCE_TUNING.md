# Host & Container Performance Tuning

Guidelines for squeezing maximum low-latency performance out of Ubuntu on AWS EC2 / OCI instances running DRADIS in Docker.

---

## Kernel Network Stack

Add to `/etc/sysctl.conf`, then apply with `sudo sysctl -p`:

```bash
# Larger socket buffers for WebSocket throughput
net.core.rmem_max=134217728
net.core.wmem_max=134217728
net.ipv4.tcp_rmem=4096 87380 134217728
net.ipv4.tcp_wmem=4096 65536 134217728

# Reduce ACK latency
net.ipv4.tcp_low_latency=1
net.ipv4.tcp_nodelay=1          # set per-socket; this documents intent
net.ipv4.tcp_sack=1

# Faster TIME_WAIT recycling
net.ipv4.tcp_tw_reuse=1
net.ipv4.tcp_fin_timeout=15

# Larger connection backlog
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=65535
```

---

## CPU Frequency Governor

Lock cores to maximum frequency — prevents the kernel from throttling mid-trade:

```bash
sudo apt install -y cpufrequtils
echo 'GOVERNOR="performance"' | sudo tee /etc/default/cpufrequtils
sudo systemctl restart cpufrequtils
# Verify
cpufreq-info | grep "current policy"
```

---

## CPU Affinity

Pin DRADIS to isolated cores, leaving cores 0–1 for the OS and kernel threads:

```bash
# Bare-metal / non-Docker
taskset -c 2,3 ./target/release/dradis

# Docker — pin container to cores 2–3, bind to local memory node
docker run -d --restart unless-stopped \
  --cpuset-cpus="2,3" \
  --cpuset-mems="0" \
  --name dradis-btc \
  --env-file .env \
  dradis
```

---

## IRQ Affinity

Route NIC interrupts away from the DRADIS cores so they stay interrupt-free:

```bash
# Find your NIC IRQs (replace eth0 with your interface, e.g. ens5 on EC2)
grep eth0 /proc/interrupts | awk '{print $1}' | tr -d ':'

# Route each IRQ to core 0 only
echo 1 | sudo tee /proc/irq/<IRQ_NUM>/smp_affinity
```

---

## Docker Ulimits

Raise file descriptor and memory-lock limits for the container:

```bash
docker run -d --restart unless-stopped \
  --cpuset-cpus="2,3" \
  --ulimit nofile=65535:65535 \
  --ulimit memlock=-1:-1 \
  --name dradis-btc \
  --env-file .env \
  dradis
```

---

## Instance Selection

| Cloud | Recommended | Avoid |
|-------|-------------|-------|
| **AWS** | `c6i` / `c7i` (compute-optimized) | `t3.*` burstable — CPU credits cause latency spikes |
| **OCI** | `VM.Standard.E4.Flex` (dedicated OCPU) or `BM.Standard.E4.128` bare-metal | Shared-core shapes |

**Region placement:** put your instance in the same region as your primary Polymarket CLOB endpoint to minimise RTT.
- Ireland (`eu-west-1`) — EU CLOB
- Canada (`ca-central-1`) — North America CLOB

