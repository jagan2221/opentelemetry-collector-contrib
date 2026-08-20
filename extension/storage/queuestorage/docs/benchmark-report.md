# QueueStorage — Segmented BBolt Benchmark Report

Issue: open-telemetry/opentelemetry-collector#15384
Fork: github.com/jagan2221/opentelemetry-collector-contrib
Date: August 2026


## Architecture

QueueStorage replaces the single-file bbolt database (filestorage) with a segmented approach: multiple fixed-size bbolt files (64MB each), managed by an LRU cache with bounded open segments.

The queue is stored as a series of segment files on disk:

    seg_000.db (64MB) → seg_001.db (64MB) → ... → seg_099.db (64MB)
    [oldest / read pointer]                        [newest / write pointer]

Only 3 segments are open (mmap'd) at any time via an LRU cache. The remaining segments sit on disk consuming zero process memory. The write segment is pinned and never evicted.


## How It Works

### Write Flow

1. Item arrives from the OTel collector's sending queue
2. Written to the active write segment (bbolt Put with NoSync=true)
3. Writes go to kernel page cache — kernel flushes to disk asynchronously (~30s)
4. When segment reaches 64MB → create new segment file, old one becomes read-only

### Read Flow (Drain)

1. Consumer requests next item → read pointer is on the oldest segment
2. If segment is not in LRU cache:
    a. Evict least-recently-used segment (bbolt.Close → munmap → RSS freed)
    b. prefetchFile(): reads the segment file sequentially via io.CopyBuffer into page cache
    c. bbolt.Open() — mmap finds all pages already warm in cache (zero page faults)
3. Read item from bbolt — hits page cache at memory speed

### Delete Flow (Segment GC)

1. All items in a segment are consumed and acknowledged
2. segment.Close() → bbolt.DB.Close() → munmap (RSS freed)
3. os.Remove("seg_N.db") → disk freed immediately
4. No compaction needed. No freelist management. Just delete the file.

### Why Sequential I/O

A queue is FIFO — reads always proceed oldest-first. This means we read segments front-to-back, one at a time.

prefetchFile() exploits this by streaming the entire segment file sequentially before bbolt opens it. The kernel sees the sequential access pattern, triggers readahead (128KB+ chunks), and the full 64MB lands in page cache via large sequential reads. When bbolt subsequently accesses pages via mmap, they're already warm — zero page faults.

Contrast with filestorage (single bbolt file): items are scattered across a B-tree. Reading one item requires traversing root → branch → leaf → value — each a random 4KB page fault to a different location in a multi-GB file. The kernel cannot predict the access pattern, so no readahead occurs.

Sequential read bandwidth on gp3 EBS: ~125 MB/s (device max)
Random 4KB read bandwidth on gp3 EBS: ~12 MB/s (IOPS-limited at 3000 IOPS x 4KB)

This 10x bandwidth gap is why segmented sequential reads fundamentally outperform single-file random reads.


## Test Setup

Machine: EC2 t3.small (2 vCPU, 2GB RAM, gp3 EBS 3000 IOPS baseline)
OS: Ubuntu 22.04
Collector: Custom OCB build with queuestorage/filestorage extension
Load: ~2KB spans via OTLP gRPC (batches of 100 spans = ~180KB per queue item)

Test topology:

    [loadgen] --OTLP gRPC--> [Collector + persistent queue] --OTLP gRPC--> [Backend :55555]

Phase 1 (Fill): Backend is down → all items queue to disk
Phase 2 (Drain): Backend starts → queue consumed, segments deleted


## Results: QueueStorage

### Fill Phase (16 minutes)

Queue size:              0 → 6.27 GB (99 segments created)
RSS (process memory):    65M – 99M (steady state ~80M)
Write throughput:        ~10-15 MB/s
Write latency:           2.5–3.5 ms (during kernel writeback flushes)
OOM killed:              No — stable on 2GB machine with 6.27GB queue

### Drain Phase (2 minutes)

Queue drained:           6.27 GB → 0.10 GB
Items drained:           ~35,700 items
RSS during drain:        43M – 105M (bounded, flat)
Read throughput:         44 MB/s (sequential)
Per-item read latency:   ~3.4 ms
Read IOPS:               ~185
Segments GC'd:           98 segments deleted (disk fully reclaimed)


## Results: FileStorage (Baseline)

### Fill Phase (8 minutes)

Queue size:              0 → 1.92 GB (single file)
RSS (process memory):    28M → 1003M (grew linearly with file size)
Write throughput:        ~9-10 MB/s
OOM killed:              Test stopped at 1003M RSS (approaching 2GB machine limit)

### Drain Phase (5 minutes)

Queue drained:           1.92 GB (file never shrinks)
Items drained:           ~10,900 items
RSS during drain:        693M → 1243M (RSS GREW during drain)
Read throughput:         5-6 MB/s (random I/O)
Per-item read latency:   ~27.5 ms
Read IOPS:               1300-1600 (random B-tree traversal)
Disk freed:              No — file stays at 1.92G, requires offline compaction


## Comparison Table

Metric                      | QueueStorage         | FileStorage          | Improvement
----------------------------|----------------------|----------------------|-------------------
RSS during fill             | 80M (6.27GB queue)   | 1003M (1.92GB queue) | 10x less memory
RSS during drain            | 43-105M (flat)       | 693-1243M (grew)     | Bounded vs unbounded
Drain throughput            | 44 MB/s              | 5-6 MB/s             | 8x faster
Per-item drain latency      | 3.4 ms               | 27.5 ms              | 8x faster
Disk reclaimed after drain  | Yes (segments deleted)| No (file stays full) | Automatic GC
Max queue on 2GB machine    | 6.27 GB+ (no limit)  | ~1.9 GB (OOM risk)   | Unbounded queue size
Write throughput            | 10-15 MB/s           | 9-10 MB/s            | Comparable


## Key Takeaways

1. Memory is bounded and independent of queue size. 80M RSS with a 6.27GB queue. FileStorage needs the entire DB in RSS (1003M for just 1.92GB).

2. Sequential I/O delivers 8x drain throughput. prefetchFile() converts random page faults into sequential readahead. 44 MB/s vs 5 MB/s.

3. Disk is reclaimed immediately. Consumed segments are deleted — no compaction step, no manual intervention. FileStorage's file never shrinks.

4. Safe on memory-constrained machines. A 2GB machine held 6.27GB of queue without OOM. FileStorage would OOM at ~2GB queue size.
