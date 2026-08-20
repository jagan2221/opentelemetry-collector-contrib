#!/bin/bash
# monitor.sh — Real-time performance monitoring for queuestorage integration test
# Usage: ./monitor.sh [pid]
#   If pid not provided, auto-detects otelcol-queuestorage-linux process

set -u

PID=${1:-$(pgrep -f otelcol-queuestorage-linux | head -1)}
if [ -z "$PID" ]; then
    echo "No otelcol-queuestorage-linux process found. Pass PID as argument."
    exit 1
fi

QUEUE_DIR="/tmp/queue_storage_test"
DEVICE=$(df "$QUEUE_DIR" 2>/dev/null | tail -1 | awk '{print $1}' | sed 's|/dev/||')
# Fallback: try to find the root device
if [ -z "$DEVICE" ] || [ ! -f "/sys/block/${DEVICE}/stat" ]; then
    DEVICE=$(lsblk -ndo NAME,TYPE | grep disk | head -1 | awk '{print $1}')
fi

echo "=== QueueStorage Performance Monitor ==="
echo "PID: $PID | Queue dir: $QUEUE_DIR | Block device: $DEVICE"
echo "========================================="
echo ""

# Column header
printf "%-8s | %-6s %-6s %-8s | %-9s %-9s %-9s %-9s | %-8s %-6s | %s\n" \
    "TIME" "RSS_MB" "FILE_MB" "HEAP_MB" \
    "R_IOPS" "W_IOPS" "R_MB/s" "W_MB/s" \
    "QUEUE_GB" "SEGS" "LATENCY"
printf "%s\n" "---------|-------------------------------|---------------------------------------------|----------------|--------"

# Read initial disk stats
read_disk_stats() {
    if [ -f "/sys/block/${DEVICE}/stat" ]; then
        cat "/sys/block/${DEVICE}/stat"
    else
        echo "0 0 0 0 0 0 0 0 0 0 0"
    fi
}

# Parse /sys/block/DEV/stat fields:
# 1=reads_completed 2=reads_merged 3=sectors_read 4=read_ms
# 5=writes_completed 6=writes_merged 7=sectors_written 8=write_ms
# 9=ios_in_progress 10=io_ms 11=weighted_io_ms
prev_stats=$(read_disk_stats)
prev_time=$(date +%s%N)

while true; do
    sleep 2

    # Check process is still alive
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "Process $PID exited."
        break
    fi

    now=$(date +%H:%M:%S)
    cur_time=$(date +%s%N)
    elapsed_ns=$((cur_time - prev_time))
    elapsed_s=$(echo "scale=3; $elapsed_ns / 1000000000" | bc)

    # --- Memory ---
    rss_kb=$(grep "VmRSS:" /proc/$PID/status 2>/dev/null | awk '{print $2}')
    rss_file_kb=$(grep "RssFile:" /proc/$PID/status 2>/dev/null | awk '{print $2}')
    # HeapInuse not easily available without /proc/PID/smaps parsing; skip
    rss_mb=$((${rss_kb:-0} / 1024))
    file_mb=$((${rss_file_kb:-0} / 1024))

    # --- Disk I/O (from /sys/block) ---
    cur_stats=$(read_disk_stats)

    prev_reads=$(echo "$prev_stats" | awk '{print $1}')
    cur_reads=$(echo "$cur_stats" | awk '{print $1}')
    prev_sectors_r=$(echo "$prev_stats" | awk '{print $3}')
    cur_sectors_r=$(echo "$cur_stats" | awk '{print $3}')
    prev_read_ms=$(echo "$prev_stats" | awk '{print $4}')
    cur_read_ms=$(echo "$cur_stats" | awk '{print $4}')

    prev_writes=$(echo "$prev_stats" | awk '{print $5}')
    cur_writes=$(echo "$cur_stats" | awk '{print $5}')
    prev_sectors_w=$(echo "$prev_stats" | awk '{print $7}')
    cur_sectors_w=$(echo "$cur_stats" | awk '{print $7}')
    prev_write_ms=$(echo "$prev_stats" | awk '{print $8}')
    cur_write_ms=$(echo "$cur_stats" | awk '{print $8}')

    delta_reads=$((cur_reads - prev_reads))
    delta_writes=$((cur_writes - prev_writes))
    delta_sectors_r=$((cur_sectors_r - prev_sectors_r))
    delta_sectors_w=$((cur_sectors_w - prev_sectors_w))
    delta_read_ms=$((cur_read_ms - prev_read_ms))
    delta_write_ms=$((cur_write_ms - prev_write_ms))

    # IOPS (per second)
    r_iops=$(echo "scale=0; $delta_reads / $elapsed_s" | bc)
    w_iops=$(echo "scale=0; $delta_writes / $elapsed_s" | bc)

    # Throughput (sectors are 512 bytes)
    r_mbs=$(echo "scale=1; $delta_sectors_r * 512 / 1048576 / $elapsed_s" | bc)
    w_mbs=$(echo "scale=1; $delta_sectors_w * 512 / 1048576 / $elapsed_s" | bc)

    # Average latency per I/O
    if [ "$delta_reads" -gt 0 ]; then
        r_lat_us=$(echo "scale=0; $delta_read_ms * 1000 / $delta_reads" | bc)
        r_lat="${r_lat_us}us"
    else
        r_lat="-"
    fi
    if [ "$delta_writes" -gt 0 ]; then
        w_lat_us=$(echo "scale=0; $delta_write_ms * 1000 / $delta_writes" | bc)
        w_lat="${w_lat_us}us"
    else
        w_lat="-"
    fi

    # --- Queue Stats ---
    if [ -d "$QUEUE_DIR" ]; then
        queue_bytes=$(du -sb "$QUEUE_DIR" 2>/dev/null | awk '{print $1}')
        queue_gb=$(echo "scale=2; ${queue_bytes:-0} / 1073741824" | bc)
        seg_count=$(find "$QUEUE_DIR" -name "seg_*.db" 2>/dev/null | wc -l)
    else
        queue_gb="0"
        seg_count="0"
    fi

    # --- Print ---
    printf "%-8s | %-6s %-6s %-8s | %-9s %-9s %-9s %-9s | %-8s %-6s | r=%s w=%s\n" \
        "$now" "${rss_mb}M" "${file_mb}M" "-" \
        "$r_iops" "$w_iops" "${r_mbs}" "${w_mbs}" \
        "${queue_gb}G" "$seg_count" "$r_lat" "$w_lat"

    # Update prev
    prev_stats="$cur_stats"
    prev_time=$cur_time
done
