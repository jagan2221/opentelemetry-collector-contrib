// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// membench is a standalone program that compares queuestorage vs filestorage
// read latency under memory pressure. Run in Docker with --memory=1g to
// demonstrate the page-fault difference.
//
// Usage:
//
//	membench --mode=populate --dir=/data --size=4GB
//	membench --mode=read --dir=/data --reads=10000
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/bbolt"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"

	queuestorage "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"
)

var (
	defaultBucket  = []byte("default")
	overrideDevice string
)

func main() {
	mode := flag.String("mode", "populate", "populate or read")
	dir := flag.String("dir", "/data", "data directory")
	sizeStr := flag.String("size", "2GB", "total data size (populate mode)")
	itemSizeKB := flag.Int("item-size", 64, "item size in KB (populate mode)")
	reads := flag.Int("reads", 10000, "number of random reads (read mode)")
	deviceFlag := flag.String("device", "", "block device name for /proc/diskstats (e.g. nvme0n1); auto-detected if empty")
	skipSegmented := flag.Bool("skip-segmented", false, "skip QueueStorage (segmented) test")
	disablePrefetch := flag.Bool("no-prefetch", false, "disable prefetchFile for comparison testing")
	flag.Parse()

	overrideDevice = *deviceFlag
	if *disablePrefetch {
		queuestorage.SetNoPrefetch(true)
	}

	switch *mode {
	case "populate":
		if err := populate(*dir, *sizeStr, *itemSizeKB); err != nil {
			fmt.Fprintf(os.Stderr, "populate failed: %v\n", err)
			os.Exit(1)
		}
	case "read":
		if err := readBench(*dir, *reads, *skipSegmented); err != nil {
			fmt.Fprintf(os.Stderr, "read failed: %v\n", err)
			os.Exit(1)
		}
	case "evict-read":
		// Evict page cache by allocating memory up to the cgroup limit, then read.
		evictPageCache()
		if err := readBench(*dir, *reads, *skipSegmented); err != nil {
			fmt.Fprintf(os.Stderr, "read failed: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}

func parseSize(s string) (int64, error) {
	multipliers := map[string]int64{"GB": 1 << 30, "MB": 1 << 20, "KB": 1 << 10}
	for suffix, mult := range multipliers {
		if len(s) > len(suffix) && s[len(s)-len(suffix):] == suffix {
			n, err := strconv.ParseInt(s[:len(s)-len(suffix)], 10, 64)
			return n * mult, err
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

func populate(dir, sizeStr string, itemSizeKB int) error {
	totalBytes, err := parseSize(sizeStr)
	if err != nil {
		return fmt.Errorf("parse size: %w", err)
	}

	itemSize := itemSizeKB * 1024
	itemCount := int(totalBytes) / itemSize

	fmt.Printf("Populating %s (%d items × %dKB)\n", sizeStr, itemCount, itemSizeKB)

	// Populate queuestorage
	qDir := filepath.Join(dir, "queuestorage")
	if err := os.MkdirAll(qDir, 0o750); err != nil {
		return err
	}

	start := time.Now()
	if err := populateQueueStorage(qDir, itemCount, itemSize); err != nil {
		return fmt.Errorf("queuestorage populate: %w", err)
	}
	fmt.Printf("  queuestorage: %v\n", time.Since(start))

	// Populate filestorage
	fDir := filepath.Join(dir, "filestorage")
	if err := os.MkdirAll(fDir, 0o750); err != nil {
		return err
	}

	start = time.Now()
	if err := populateFileStorage(filepath.Join(fDir, "single.db"), itemCount, itemSize); err != nil {
		return fmt.Errorf("filestorage populate: %w", err)
	}
	fmt.Printf("  filestorage:  %v\n", time.Since(start))

	// Write item count for the read phase.
	return os.WriteFile(filepath.Join(dir, "item_count"), []byte(strconv.Itoa(itemCount)), 0o644)
}

func readBench(dir string, numReads int, skipSegmented bool) error {
	countBytes, err := os.ReadFile(filepath.Join(dir, "item_count"))
	if err != nil {
		return fmt.Errorf("read item_count: %w (did you run --mode=populate first?)", err)
	}
	itemCount, _ := strconv.Atoi(string(countBytes))
	fmt.Printf("Reading %d random items from %d total items\n", numReads, itemCount)

	// Sequential indices (real queue pattern: read oldest first).
	seqIndices := make([]uint64, numReads)
	for i := range seqIndices {
		seqIndices[i] = uint64(i)
	}

	// Detect block device for IOPS measurement.
	device := findRootDevice(dir)
	fmt.Printf("Block device: %s\n", device)

	// --- QueueStorage reads ---
	if !skipSegmented {
		qDir := filepath.Join(dir, "queuestorage")
		fmt.Println("\n--- QueueStorage (segmented) ---")

		diskBefore := readDiskStats(device)
		startupStart := time.Now()
		client, err := queuestorage.NewQueueClientForBench(zap.NewNop(), qDir, 64*1024*1024, 3)
		if err != nil {
			return fmt.Errorf("open queuestorage: %w", err)
		}
		startupDur := time.Since(startupStart)
		diskAfter := readDiskStats(device)
		fmt.Printf("  Startup: %v\n", startupDur)
		printDiskIOPS("  Startup", diskBefore, diskAfter, startupDur)

		printMemStats("  After open")

		fmt.Println("  Sequential reads (real queue pattern):")
		ioBefore := readIOCounters()
		faultsBefore := readMajorFaults()
		diskBefore = readDiskStats(device)
		seqStart := time.Now()
		latencies := measureReads(client, seqIndices)
		seqElapsed := time.Since(seqStart)
		ioAfter := readIOCounters()
		faultsAfter := readMajorFaults()
		diskAfter = readDiskStats(device)
		printLatencyStats("    ", latencies)
		printIODelta("    ", ioBefore, ioAfter, numReads)
		fmt.Printf("    major_faults: %d (each = 1 disk page read)\n", faultsAfter-faultsBefore)
		printDiskIOPS("    ", diskBefore, diskAfter, seqElapsed)

		client.Close(context.Background())
	} else {
		fmt.Println("\n--- QueueStorage (segmented) --- SKIPPED")
	}

	// --- FileStorage reads ---
	dbPath := filepath.Join(dir, "filestorage", "single.db")
	fmt.Println("\n--- FileStorage (single bbolt) ---")

	diskBefore := readDiskStats(device)
	startupStart := time.Now()
	opts := &bbolt.Options{
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	}
	db, err := bbolt.Open(dbPath, 0o600, opts)
	if err != nil {
		return fmt.Errorf("open filestorage: %w", err)
	}
	startupDur := time.Since(startupStart)
	diskAfter := readDiskStats(device)
	fmt.Printf("  Startup: %v\n", startupDur)
	printDiskIOPS("  Startup", diskBefore, diskAfter, startupDur)

	printMemStats("  After open")

	fmt.Println("  Sequential reads:")
	ioBefore := readIOCounters()
	faultsBefore := readMajorFaults()
	diskBefore = readDiskStats(device)
	seqStart := time.Now()
	latencies := measureFileStorageReads(db, seqIndices)
	seqElapsed := time.Since(seqStart)
	ioAfter := readIOCounters()
	faultsAfter := readMajorFaults()
	diskAfter = readDiskStats(device)
	printLatencyStats("    ", latencies)
	printIODelta("    ", ioBefore, ioAfter, numReads)
	fmt.Printf("    major_faults: %d (each = 1 disk page read)\n", faultsAfter-faultsBefore)
	printDiskIOPS("    ", diskBefore, diskAfter, seqElapsed)

	db.Close()

	return nil
}

const progressInterval = 5000

func measureReads(client interface{ Get(context.Context, string) ([]byte, error) }, indices []uint64) []time.Duration {
	ctx := context.Background()
	latencies := make([]time.Duration, 0, len(indices))
	batchStart := time.Now()
	for i, idx := range indices {
		key := strconv.FormatUint(idx, 10)
		start := time.Now()
		_, _ = client.Get(ctx, key)
		latencies = append(latencies, time.Since(start))

		if (i+1)%progressInterval == 0 {
			batchElapsed := time.Since(batchStart)
			batchLatencies := latencies[i+1-progressInterval : i+1]
			printProgressStats(i+1, len(indices), batchLatencies, batchElapsed)
			batchStart = time.Now()
		}
	}
	// Print final partial batch if any.
	remainder := len(indices) % progressInterval
	if remainder > 0 && len(indices) > progressInterval {
		batchElapsed := time.Since(batchStart)
		batchLatencies := latencies[len(latencies)-remainder:]
		printProgressStats(len(indices), len(indices), batchLatencies, batchElapsed)
	}
	return latencies
}

func printProgressStats(done, total int, batch []time.Duration, elapsed time.Duration) {
	sort.Slice(batch, func(i, j int) bool { return batch[i] < batch[j] })
	n := len(batch)
	var sum time.Duration
	for _, l := range batch {
		sum += l
	}
	rss, rssFile := readProcessMemory()
	fmt.Printf("    [%d/%d] avg=%v p50=%v p99=%v throughput=%.0f items/s rss=%dMB rss_file=%dMB\n",
		done, total, sum/time.Duration(n), batch[n/2], batch[n*99/100],
		float64(n)/elapsed.Seconds(), rss/1024, rssFile/1024)
}

// readProcessMemory returns VmRSS and RssFile in KB from /proc/self/status.
func readProcessMemory() (rssKB, rssFileKB int64) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "VmRSS:":
			rssKB = val
		case "RssFile:":
			rssFileKB = val
		}
	}
	return rssKB, rssFileKB
}

func measureFileStorageReads(db *bbolt.DB, indices []uint64) []time.Duration {
	latencies := make([]time.Duration, 0, len(indices))
	batchStart := time.Now()
	for i, idx := range indices {
		key := []byte(strconv.FormatUint(idx, 10))
		start := time.Now()
		_ = db.View(func(tx *bbolt.Tx) error {
			tx.Bucket(defaultBucket).Get(key)
			return nil
		})
		latencies = append(latencies, time.Since(start))

		if (i+1)%progressInterval == 0 {
			batchElapsed := time.Since(batchStart)
			batchLatencies := latencies[i+1-progressInterval : i+1]
			printProgressStats(i+1, len(indices), batchLatencies, batchElapsed)
			batchStart = time.Now()
		}
	}
	remainder := len(indices) % progressInterval
	if remainder > 0 && len(indices) > progressInterval {
		batchElapsed := time.Since(batchStart)
		batchLatencies := latencies[len(latencies)-remainder:]
		printProgressStats(len(indices), len(indices), batchLatencies, batchElapsed)
	}
	return latencies
}

func printLatencyStats(prefix string, latencies []time.Duration) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	fmt.Printf("%s latency (n=%d):\n", prefix, n)
	fmt.Printf("    p50:  %v\n", latencies[n*50/100])
	fmt.Printf("    p90:  %v\n", latencies[n*90/100])
	fmt.Printf("    p99:  %v\n", latencies[n*99/100])
	fmt.Printf("    max:  %v\n", latencies[n-1])

	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	fmt.Printf("    avg:  %v\n", total/time.Duration(n))
}

func printMemStats(prefix string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("%s: HeapInuse=%.1fMiB, Sys=%.1fMiB\n",
		prefix, float64(m.HeapInuse)/(1024*1024), float64(m.Sys)/(1024*1024))
}

// ioCounters captures /proc/self/io stats (Linux only).
type ioCounters struct {
	readBytes  int64 // actual bytes fetched from storage (includes mmap page faults)
	writeBytes int64
	syscr      int64 // read syscalls
	syscw      int64 // write syscalls
}

func readIOCounters() ioCounters {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return ioCounters{}
	}
	defer f.Close()

	var c ioCounters
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue
		}
		val, _ := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		switch parts[0] {
		case "read_bytes":
			c.readBytes = val
		case "write_bytes":
			c.writeBytes = val
		case "syscr":
			c.syscr = val
		case "syscw":
			c.syscw = val
		}
	}
	return c
}

func printIODelta(prefix string, before, after ioCounters, numOps int) {
	readBytes := after.readBytes - before.readBytes
	readOps := after.syscr - before.syscr
	fmt.Printf("%s I/O:\n", prefix)
	fmt.Printf("    read_bytes: %.2f MiB (actual storage reads, incl. mmap page-ins)\n", float64(readBytes)/(1024*1024))
	fmt.Printf("    read_iops:  %d (syscalls)\n", readOps)
	if numOps > 0 {
		fmt.Printf("    bytes/read: %.1f KB per operation\n", float64(readBytes)/float64(numOps)/1024)
	}
}

// diskStats captures block device I/O counters from /proc/diskstats.
type diskStats struct {
	readsCompleted  int64
	sectorsRead     int64 // each sector = 512 bytes
	msReading       int64
	writesCompleted int64
	sectorsWritten  int64
	msWriting       int64
}

// findRootDevice finds the block device backing the given path.
func findRootDevice(path string) string {
	if overrideDevice != "" {
		return overrideDevice
	}

	// Read /proc/mounts to find the device for the given path.
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return ""
	}
	var bestMount, bestDevice string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		mountPoint := fields[1]
		if strings.HasPrefix(path, mountPoint) && len(mountPoint) > len(bestMount) {
			bestMount = mountPoint
			bestDevice = fields[0]
		}
	}
	// Extract device name (e.g., /dev/nvme0n1p1 → nvme0n1p1).
	dev := strings.TrimPrefix(bestDevice, "/dev/")

	// Handle device-mapper names like "root" — resolve via /sys/block or
	// fall back to scanning /proc/diskstats for the first real device.
	if dev == "root" || dev == "" {
		dev = findFirstBlockDevice()
	}
	return dev
}

// findFirstBlockDevice returns the first real block device from /proc/diskstats
// that looks like a disk (nvme*, xvd*, sd*).
func findFirstBlockDevice() string {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if strings.HasPrefix(name, "nvme") || strings.HasPrefix(name, "xvd") || strings.HasPrefix(name, "sd") {
			// Prefer the whole-disk device (no partition number suffix).
			if !strings.ContainsAny(name[len(name)-1:], "0123456789") || strings.HasPrefix(name, "nvme") {
				return name
			}
		}
	}
	// If nothing found without partition, try first entry with a partition.
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if strings.HasPrefix(name, "nvme") || strings.HasPrefix(name, "xvd") || strings.HasPrefix(name, "sd") {
			return name
		}
	}
	return ""
}

func readDiskStats(device string) diskStats {
	if device == "" {
		return diskStats{}
	}
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return diskStats{}
	}
	// Also check the parent device (e.g., nvme0n1p1 → nvme0n1).
	parentDev := device
	for i := len(device) - 1; i >= 0; i-- {
		if device[i] == 'p' && i > 0 && device[i-1] >= '0' && device[i-1] <= '9' {
			parentDev = device[:i]
			break
		}
	}

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if name != device && name != parentDev {
			continue
		}
		reads, _ := strconv.ParseInt(fields[3], 10, 64)
		sectorsRead, _ := strconv.ParseInt(fields[5], 10, 64)
		msRead, _ := strconv.ParseInt(fields[6], 10, 64)
		writes, _ := strconv.ParseInt(fields[7], 10, 64)
		sectorsWritten, _ := strconv.ParseInt(fields[9], 10, 64)
		msWrite, _ := strconv.ParseInt(fields[10], 10, 64)
		return diskStats{
			readsCompleted:  reads,
			sectorsRead:     sectorsRead,
			msReading:       msRead,
			writesCompleted: writes,
			sectorsWritten:  sectorsWritten,
			msWriting:       msWrite,
		}
	}
	return diskStats{}
}

func printDiskIOPS(prefix string, before, after diskStats, elapsed time.Duration) {
	readOps := after.readsCompleted - before.readsCompleted
	writeOps := after.writesCompleted - before.writesCompleted
	readMB := float64(after.sectorsRead-before.sectorsRead) * 512 / (1024 * 1024)
	writeMB := float64(after.sectorsWritten-before.sectorsWritten) * 512 / (1024 * 1024)
	elapsedSec := elapsed.Seconds()

	readIOPS := float64(readOps) / elapsedSec
	writeIOPS := float64(writeOps) / elapsedSec
	totalIOPS := readIOPS + writeIOPS

	fmt.Printf("%s disk I/O (from /proc/diskstats):\n", prefix)
	fmt.Printf("    read_ops:    %d (%.0f IOPS)\n", readOps, readIOPS)
	fmt.Printf("    write_ops:   %d (%.0f IOPS)\n", writeOps, writeIOPS)
	fmt.Printf("    total_iops:  %.0f (gp3 baseline: 3000)\n", totalIOPS)
	fmt.Printf("    read_data:   %.2f MiB (%.1f MiB/s)\n", readMB, readMB/elapsedSec)
	fmt.Printf("    write_data:  %.2f MiB (%.1f MiB/s)\n", writeMB, writeMB/elapsedSec)
	fmt.Printf("    gp3_usage:   %.1f%% of 3000 IOPS baseline\n", totalIOPS/3000*100)
}

// readMajorFaults reads major page faults from /proc/self/stat.
func readMajorFaults() int64 {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	// Field 12 (0-indexed) is majflt (after the comm field in parens).
	// Find closing paren first, then count fields.
	s := string(data)
	idx := strings.LastIndex(s, ")")
	if idx < 0 {
		return 0
	}
	fields := strings.Fields(s[idx+2:])
	if len(fields) < 10 {
		return 0
	}
	// majflt is field index 9 after the closing paren (0-indexed: state=0, ppid=1, pgrp=2, session=3, tty=4, tpgid=5, flags=6, minflt=7, cminflt=8, majflt=9)
	val, _ := strconv.ParseInt(fields[9], 10, 64)
	return val
}

// evictPageCache allocates and touches ~900MB of memory to push file-backed
// pages out of the cgroup's page cache, then releases it.
func evictPageCache() {
	fmt.Println("Evicting page cache by allocating 900MB...")
	const chunkSize = 64 * 1024 * 1024 // 64MB chunks
	const numChunks = 14               // ~900MB
	chunks := make([][]byte, numChunks)
	for i := range chunks {
		chunks[i] = make([]byte, chunkSize)
		// Touch every page to force allocation.
		for j := 0; j < chunkSize; j += 4096 {
			chunks[i][j] = byte(i)
		}
	}
	// Release.
	chunks = nil
	runtime.GC()
	fmt.Println("Page cache evicted.")
}

func populateQueueStorage(dir string, itemCount, itemSize int) error {
	client, err := queuestorage.NewQueueClientForBench(zap.NewNop(), dir, 64*1024*1024, 3)
	if err != nil {
		return err
	}
	defer client.Close(context.Background())

	ctx := context.Background()
	payload := make([]byte, itemSize)

	// Batch writes (100 items per Batch call) for speed.
	batchSize := 100
	for start := 0; start < itemCount; start += batchSize {
		end := start + batchSize
		if end > itemCount {
			end = itemCount
		}
		ops := make([]*storage.Operation, 0, end-start)
		for i := start; i < end; i++ {
			binary.LittleEndian.PutUint64(payload, uint64(i))
			val := make([]byte, itemSize)
			copy(val, payload)
			ops = append(ops, storage.SetOperation(strconv.FormatUint(uint64(i), 10), val))
		}
		if err := client.Batch(ctx, ops...); err != nil {
			return err
		}
		if end%10000 == 0 {
			fmt.Printf("    queuestorage: %d/%d items (%.1f%%)\n", end, itemCount, float64(end)/float64(itemCount)*100)
		}
	}
	return nil
}

func populateFileStorage(dbPath string, itemCount, itemSize int) error {
	opts := &bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	}
	db, err := bbolt.Open(dbPath, 0o600, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		return err
	}); err != nil {
		return err
	}

	// Batch writes for speed (100 items per transaction).
	batchSize := 100
	payload := make([]byte, itemSize)
	for start := 0; start < itemCount; start += batchSize {
		end := start + batchSize
		if end > itemCount {
			end = itemCount
		}
		if err := db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(defaultBucket)
			for i := start; i < end; i++ {
				binary.LittleEndian.PutUint64(payload, uint64(i))
				if err := bucket.Put([]byte(strconv.FormatUint(uint64(i), 10)), payload); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if end%10000 == 0 {
			fmt.Printf("    filestorage:  %d/%d items (%.1f%%)\n", end, itemCount, float64(end)/float64(itemCount)*100)
		}
	}
	return nil
}
