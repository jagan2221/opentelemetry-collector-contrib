// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

// --- Helpers for the filestorage baseline ---

// fileStorageBaseline is a minimal single-bbolt client matching filestorage behavior.
type fileStorageBaseline struct {
	db *bbolt.DB
}

func newFileStorageBaseline(path string) (*fileStorageBaseline, error) {
	opts := &bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	}
	db, err := bbolt.Open(path, 0o600, opts)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &fileStorageBaseline{db: db}, nil
}

func (f *fileStorageBaseline) Batch(ops ...*storage.Operation) error {
	return f.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		for _, op := range ops {
			switch op.Type {
			case storage.Get:
				v := bucket.Get([]byte(op.Key))
				if v != nil {
					op.Value = make([]byte, len(v))
					copy(op.Value, v)
				}
			case storage.Set:
				if err := bucket.Put([]byte(op.Key), op.Value); err != nil {
					return err
				}
			case storage.Delete:
				if err := bucket.Delete([]byte(op.Key)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (f *fileStorageBaseline) Close() error {
	return f.db.Close()
}

// --- Offer Throughput ---

func BenchmarkOfferThroughput_QueueStorage(b *testing.B) {
	dir := b.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	require.NoError(b, err)
	b.Cleanup(func() { _ = client.Close(context.Background()) })

	ctx := context.Background()
	payload := make([]byte, 1024)
	meta := []byte("metadata-placeholder-bytes-here")

	b.ResetTimer()
	for i := range b.N {
		key := strconv.FormatUint(uint64(i), 10)
		if err := client.Batch(ctx,
			storage.SetOperation("qmv0", meta),
			storage.SetOperation(key, payload),
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOfferThroughput_FileStorage(b *testing.B) {
	dir := b.TempDir()
	fs, err := newFileStorageBaseline(filepath.Join(dir, "single.db"))
	require.NoError(b, err)
	b.Cleanup(func() { _ = fs.Close() })

	payload := make([]byte, 1024)
	meta := []byte("metadata-placeholder-bytes-here")

	b.ResetTimer()
	for i := range b.N {
		key := strconv.FormatUint(uint64(i), 10)
		if err := fs.Batch(
			storage.SetOperation("qmv0", meta),
			storage.SetOperation(key, payload),
		); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Read + Ack Cycle ---

func BenchmarkReadAckCycle_QueueStorage(b *testing.B) {
	dir := b.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	require.NoError(b, err)
	b.Cleanup(func() { _ = client.Close(context.Background()) })

	ctx := context.Background()
	payload := make([]byte, 1024)

	// Pre-fill
	for i := range b.N {
		key := strconv.FormatUint(uint64(i), 10)
		require.NoError(b, client.Set(ctx, key, payload))
	}
	require.NoError(b, client.Set(ctx, "qmv0", []byte("meta")))

	var peak uint64
	b.ResetTimer()
	for i := range b.N {
		key := strconv.FormatUint(uint64(i), 10)
		getOp := storage.GetOperation(key)
		if err := client.Batch(ctx,
			storage.SetOperation("qmv0", []byte("meta")),
			getOp,
		); err != nil {
			b.Fatal(err)
		}
		if err := client.Batch(ctx,
			storage.SetOperation("qmv0", []byte("meta")),
			storage.DeleteOperation(key),
		); err != nil {
			b.Fatal(err)
		}
		logMemory(b, i+1, 100, &peak)
	}
	b.StopTimer()
	b.ReportMetric(float64(peak)/(1024*1024), "peak_heap_MiB")
}

func BenchmarkReadAckCycle_FileStorage(b *testing.B) {
	dir := b.TempDir()
	fs, err := newFileStorageBaseline(filepath.Join(dir, "single.db"))
	require.NoError(b, err)
	b.Cleanup(func() { _ = fs.Close() })

	payload := make([]byte, 1024)

	// Pre-fill
	for i := range b.N {
		key := strconv.FormatUint(uint64(i), 10)
		require.NoError(b, fs.Batch(storage.SetOperation(key, payload)))
	}
	require.NoError(b, fs.Batch(storage.SetOperation("qmv0", []byte("meta"))))

	var peak uint64
	b.ResetTimer()
	for i := range b.N {
		key := strconv.FormatUint(uint64(i), 10)
		getOp := storage.GetOperation(key)
		if err := fs.Batch(
			storage.SetOperation("qmv0", []byte("meta")),
			getOp,
		); err != nil {
			b.Fatal(err)
		}
		if err := fs.Batch(
			storage.SetOperation("qmv0", []byte("meta")),
			storage.DeleteOperation(key),
		); err != nil {
			b.Fatal(err)
		}
		logMemory(b, i+1, 100, &peak)
	}
	b.StopTimer()
	b.ReportMetric(float64(peak)/(1024*1024), "peak_heap_MiB")
}

// --- Startup Time (the key benchmark for the issue) ---

func populateQueueStorage(dir string, itemCount int, itemSize int) error {
	client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	if err != nil {
		return err
	}
	ctx := context.Background()
	payload := make([]byte, itemSize)
	for i := range itemCount {
		key := strconv.FormatUint(uint64(i), 10)
		if err := client.Batch(ctx,
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("wi=%d", i+1))),
			storage.SetOperation(key, payload),
		); err != nil {
			_ = client.Close(ctx)
			return err
		}
	}
	return client.Close(ctx)
}

func populateFileStorage(path string, itemCount int, itemSize int) error {
	fs, err := newFileStorageBaseline(path)
	if err != nil {
		return err
	}
	payload := make([]byte, itemSize)
	for i := range itemCount {
		key := strconv.FormatUint(uint64(i), 10)
		if err := fs.Batch(
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("wi=%d", i+1))),
			storage.SetOperation(key, payload),
		); err != nil {
			_ = fs.Close()
			return err
		}
	}
	return fs.Close()
}

func BenchmarkStartup_QueueStorage_1GB(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// 1GB = 16K items × 64KB each
	itemCount := 16 * 1024
	itemSize := 64 * 1024
	dir := b.TempDir()

	b.Log("Populating 1GB queue storage...")
	require.NoError(b, populateQueueStorage(dir, itemCount, itemSize))

	var m1, m2 runtime.MemStats
	b.ResetTimer()

	for range b.N {
		runtime.GC()
		runtime.ReadMemStats(&m1)

		client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
		require.NoError(b, err)

		runtime.ReadMemStats(&m2)
		b.ReportMetric(float64(m2.HeapInuse-m1.HeapInuse)/(1024*1024), "heap_MiB")

		b.StopTimer()
		require.NoError(b, client.Close(context.Background()))
		b.StartTimer()
	}
}

func BenchmarkStartup_FileStorage_1GB(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// 1GB = 16K items × 64KB each
	itemCount := 16 * 1024
	itemSize := 64 * 1024
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "single.db")

	b.Log("Populating 1GB file storage...")
	require.NoError(b, populateFileStorage(dbPath, itemCount, itemSize))

	var m1, m2 runtime.MemStats
	b.ResetTimer()

	for range b.N {
		runtime.GC()
		runtime.ReadMemStats(&m1)

		opts := &bbolt.Options{
			NoSync:         true,
			NoFreelistSync: true,
			FreelistType:   bbolt.FreelistMapType,
		}
		db, err := bbolt.Open(dbPath, 0o600, opts)
		require.NoError(b, err)

		runtime.ReadMemStats(&m2)
		b.ReportMetric(float64(m2.HeapInuse-m1.HeapInuse)/(1024*1024), "heap_MiB")

		b.StopTimer()
		require.NoError(b, db.Close())
		b.StartTimer()
	}
}

// --- Memory at Rest (large queue exists, measure memory while open) ---

func BenchmarkMemoryAtRest_QueueStorage_1GB(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	itemCount := 16 * 1024
	itemSize := 64 * 1024
	dir := b.TempDir()

	b.Log("Populating 1GB queue storage...")
	require.NoError(b, populateQueueStorage(dir, itemCount, itemSize))

	client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	require.NoError(b, err)

	// Simulate reading a few items (triggers opening read segment).
	ctx := context.Background()
	for i := range 10 {
		_, _ = client.Get(ctx, strconv.FormatUint(uint64(i), 10))
	}

	// Measure actual mmapped bytes (sum of open segment file sizes).
	var mmappedBytes int64
	for _, seg := range client.registry.openList {
		mmappedBytes += seg.fileSize()
	}
	mmappedMiB := float64(mmappedBytes) / (1024 * 1024)
	b.ReportMetric(mmappedMiB, "mmap_MiB")
	b.Logf("Queue storage: %d open segments, %.2f MiB mmapped (out of 1GB total)", len(client.registry.openList), mmappedMiB)

	require.NoError(b, client.Close(ctx))
}

func BenchmarkMemoryAtRest_FileStorage_1GB(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	itemCount := 16 * 1024
	itemSize := 64 * 1024
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "single.db")

	b.Log("Populating 1GB file storage...")
	require.NoError(b, populateFileStorage(dbPath, itemCount, itemSize))

	opts := &bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	}
	db, err := bbolt.Open(dbPath, 0o600, opts)
	require.NoError(b, err)

	// Simulate reading a few items.
	_ = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		for i := range 10 {
			bucket.Get([]byte(strconv.FormatUint(uint64(i), 10)))
		}
		return nil
	})

	// File storage mmaps the entire DB file.
	info, err := os.Stat(dbPath)
	require.NoError(b, err)
	mmappedMiB := float64(info.Size()) / (1024 * 1024)
	b.ReportMetric(mmappedMiB, "mmap_MiB")
	b.Logf("File storage: entire DB mmapped, %.2f MiB", mmappedMiB)

	require.NoError(b, db.Close())
}

// --- Segment GC Speed ---

func BenchmarkSegmentGC(b *testing.B) {
	dir := b.TempDir()
	ctx := context.Background()

	// Create multiple segments worth of data.
	client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	require.NoError(b, err)

	payload := make([]byte, 64*1024)
	// Write enough for ~4 segments (4 × 64MB / 64KB per item = 4096 items).
	for i := range 4096 {
		key := strconv.FormatUint(uint64(i), 10)
		require.NoError(b, client.Set(ctx, key, payload))
	}
	require.NoError(b, client.Close(ctx))

	// Reopen and benchmark deletion (GC) of consumed segments.
	client, err = newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	require.NoError(b, err)
	b.Cleanup(func() { _ = client.Close(ctx) })

	b.ResetTimer()
	for i := range b.N {
		idx := i % 4096
		key := strconv.FormatUint(uint64(idx), 10)
		_ = client.Delete(ctx, key)
	}
}

// --- Startup with 10GB (run only manually, not in short mode) ---

func BenchmarkStartup_QueueStorage_10GB(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10GB benchmark in short mode")
	}
	if os.Getenv("BENCH_LARGE") == "" {
		b.Skip("set BENCH_LARGE=1 to run 10GB benchmarks")
	}

	// 10GB = 160K items × 64KB each
	itemCount := 160 * 1024
	itemSize := 64 * 1024
	dir := b.TempDir()

	b.Log("Populating 10GB queue storage (this takes several minutes)...")
	start := time.Now()
	require.NoError(b, populateQueueStorage(dir, itemCount, itemSize))
	b.Logf("Population took %v", time.Since(start))

	var m1, m2 runtime.MemStats
	b.ResetTimer()

	for range b.N {
		runtime.GC()
		runtime.ReadMemStats(&m1)

		client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
		require.NoError(b, err)

		runtime.ReadMemStats(&m2)
		b.ReportMetric(float64(m2.HeapInuse-m1.HeapInuse)/(1024*1024), "heap_MiB")

		b.StopTimer()
		require.NoError(b, client.Close(context.Background()))
		b.StartTimer()
	}
}

func BenchmarkStartup_FileStorage_10GB(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10GB benchmark in short mode")
	}
	if os.Getenv("BENCH_LARGE") == "" {
		b.Skip("set BENCH_LARGE=1 to run 10GB benchmarks")
	}

	itemCount := 160 * 1024
	itemSize := 64 * 1024
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "single.db")

	b.Log("Populating 10GB file storage (this takes several minutes)...")
	start := time.Now()
	require.NoError(b, populateFileStorage(dbPath, itemCount, itemSize))
	b.Logf("Population took %v", time.Since(start))

	var m1, m2 runtime.MemStats
	b.ResetTimer()

	for range b.N {
		runtime.GC()
		runtime.ReadMemStats(&m1)

		opts := &bbolt.Options{
			NoSync:         true,
			NoFreelistSync: true,
			FreelistType:   bbolt.FreelistMapType,
			Timeout:        5 * time.Second,
		}
		db, err := bbolt.Open(dbPath, 0o600, opts)
		require.NoError(b, err)

		runtime.ReadMemStats(&m2)
		b.ReportMetric(float64(m2.HeapInuse-m1.HeapInuse)/(1024*1024), "heap_MiB")

		b.StopTimer()
		require.NoError(b, db.Close())
		b.StartTimer()
	}
}

// --- Helpers for comprehensive benchmarks ---

func logMemory(b *testing.B, iteration, interval int, peak *uint64) {
	if iteration%interval != 0 {
		return
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.HeapInuse > *peak {
		*peak = m.HeapInuse
	}
	b.Logf("[%d] heap=%dMB sys=%dMB", iteration, m.HeapInuse/(1024*1024), m.Sys/(1024*1024))
}

func reportLatencyPercentiles(b *testing.B, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	b.ReportMetric(float64(latencies[n/2].Microseconds()), "p50_us")
	b.ReportMetric(float64(latencies[n*99/100].Microseconds()), "p99_us")
}

// --- Write Throughput ---

func BenchmarkWriteThroughput_QueueStorage(b *testing.B) {
	for _, itemSize := range []int{4 * 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("item_%dKB", itemSize/1024), func(b *testing.B) {
			dir := b.TempDir()
			client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
			require.NoError(b, err)
			b.Cleanup(func() { _ = client.Close(context.Background()) })

			ctx := context.Background()
			payload := make([]byte, itemSize)
			meta := []byte("metadata-placeholder")

			b.Logf("Config: item_size=%dKB", itemSize/1024)

			var peak uint64
			latencies := make([]time.Duration, 0, b.N)

			b.ResetTimer()
			for i := range b.N {
				key := strconv.FormatUint(uint64(i), 10)
				start := time.Now()
				if err := client.Batch(ctx,
					storage.SetOperation("qmv0", meta),
					storage.SetOperation(key, payload),
				); err != nil {
					b.Fatal(err)
				}
				latencies = append(latencies, time.Since(start))
				logMemory(b, i+1, 100, &peak)
			}
			b.StopTimer()

			elapsed := b.Elapsed()
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "writes/sec")
			b.ReportMetric(float64(b.N)*float64(itemSize)/(1024*1024)/elapsed.Seconds(), "write_MiB/s")
			b.ReportMetric(float64(peak)/(1024*1024), "peak_heap_MiB")
			reportLatencyPercentiles(b, latencies)
		})
	}
}

func BenchmarkWriteThroughput_FileStorage(b *testing.B) {
	for _, itemSize := range []int{4 * 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("item_%dKB", itemSize/1024), func(b *testing.B) {
			dir := b.TempDir()
			fs, err := newFileStorageBaseline(filepath.Join(dir, "single.db"))
			require.NoError(b, err)
			b.Cleanup(func() { _ = fs.Close() })

			payload := make([]byte, itemSize)
			meta := []byte("metadata-placeholder")

			b.Logf("Config: item_size=%dKB", itemSize/1024)

			var peak uint64
			latencies := make([]time.Duration, 0, b.N)

			b.ResetTimer()
			for i := range b.N {
				key := strconv.FormatUint(uint64(i), 10)
				start := time.Now()
				if err := fs.Batch(
					storage.SetOperation("qmv0", meta),
					storage.SetOperation(key, payload),
				); err != nil {
					b.Fatal(err)
				}
				latencies = append(latencies, time.Since(start))
				logMemory(b, i+1, 100, &peak)
			}
			b.StopTimer()

			elapsed := b.Elapsed()
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "writes/sec")
			b.ReportMetric(float64(b.N)*float64(itemSize)/(1024*1024)/elapsed.Seconds(), "write_MiB/s")
			b.ReportMetric(float64(peak)/(1024*1024), "peak_heap_MiB")
			reportLatencyPercentiles(b, latencies)
		})
	}
}

// --- Read Throughput (sequential consume pattern) ---

func BenchmarkReadThroughput_QueueStorage(b *testing.B) {
	for _, itemSize := range []int{4 * 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("item_%dKB", itemSize/1024), func(b *testing.B) {
			dir := b.TempDir()

			// Fixed item count — 10000 items gives meaningful latency distribution
			// without exhausting disk (4KB×10K=40MB, 64KB×10K=640MB).
			const itemCount = 10000
			require.NoError(b, populateQueueStorage(dir, itemCount, itemSize))

			totalSizeMB := float64(itemCount*itemSize) / (1024 * 1024)
			b.Logf("Config: item_size=%dKB items=%d total_queue=%.1fMB",
				itemSize/1024, itemCount, totalSizeMB)

			// Open for reading.
			client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
			require.NoError(b, err)
			b.Cleanup(func() { _ = client.Close(context.Background()) })

			// Report mmap exposure: only open segments are mmap'd.
			var totalOnDisk int64
			for _, seg := range client.registry.segments {
				totalOnDisk += seg.fileSize()
			}
			var mmapped int64
			for _, seg := range client.registry.openList {
				mmapped += seg.fileSize()
			}
			b.Logf("Segments: %d total (%.1f MiB on disk), %d open (%.1f MiB mmap'd)",
				len(client.registry.segments), float64(totalOnDisk)/(1024*1024),
				len(client.registry.openList), float64(mmapped)/(1024*1024))

			ctx := context.Background()
			var peak uint64

			b.ResetTimer()
			for range b.N {
				latencies := make([]time.Duration, 0, itemCount)
				iterStart := time.Now()
				for i := range itemCount {
					key := strconv.FormatUint(uint64(i), 10)
					start := time.Now()
					_, _ = client.Get(ctx, key)
					latencies = append(latencies, time.Since(start))
					logMemory(b, i+1, 100, &peak)
				}
				iterElapsed := time.Since(iterStart)

				// Measure mmap after reads (segments may have been opened/evicted).
				var mmapAfter int64
				for _, seg := range client.registry.openList {
					mmapAfter += seg.fileSize()
				}
				b.ReportMetric(float64(mmapAfter)/(1024*1024), "mmap_MiB")
				b.ReportMetric(float64(totalOnDisk)/(1024*1024), "total_disk_MiB")
				b.ReportMetric(float64(itemCount)/iterElapsed.Seconds(), "reads/sec")
				b.ReportMetric(float64(itemCount)*float64(itemSize)/(1024*1024)/iterElapsed.Seconds(), "read_MiB/s")
				b.ReportMetric(float64(peak)/(1024*1024), "peak_heap_MiB")
				b.ReportMetric(float64(itemCount), "total_items")
				b.Logf("Total: %d items read in %v", itemCount, iterElapsed)
				reportLatencyPercentiles(b, latencies)
			}
		})
	}
}

func BenchmarkReadThroughput_FileStorage(b *testing.B) {
	for _, itemSize := range []int{4 * 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("item_%dKB", itemSize/1024), func(b *testing.B) {
			dir := b.TempDir()
			dbPath := filepath.Join(dir, "single.db")

			const itemCount = 10000
			require.NoError(b, populateFileStorage(dbPath, itemCount, itemSize))

			totalSizeMB := float64(itemCount*itemSize) / (1024 * 1024)
			b.Logf("Config: item_size=%dKB items=%d total_queue=%.1fMB",
				itemSize/1024, itemCount, totalSizeMB)

			// Open for reading.
			opts := &bbolt.Options{
				NoSync:         true,
				NoFreelistSync: true,
				FreelistType:   bbolt.FreelistMapType,
			}
			db, err := bbolt.Open(dbPath, 0o600, opts)
			require.NoError(b, err)
			b.Cleanup(func() { _ = db.Close() })

			// FileStorage mmaps the ENTIRE DB file.
			dbInfo, err := os.Stat(dbPath)
			require.NoError(b, err)
			mmapMiB := float64(dbInfo.Size()) / (1024 * 1024)
			b.Logf("Single DB: %.1f MiB (entirely mmap'd)", mmapMiB)

			var peak uint64

			b.ResetTimer()
			for range b.N {
				latencies := make([]time.Duration, 0, itemCount)
				iterStart := time.Now()
				for i := range itemCount {
					key := strconv.FormatUint(uint64(i), 10)
					start := time.Now()
					_ = db.View(func(tx *bbolt.Tx) error {
						bucket := tx.Bucket(defaultBucket)
						v := bucket.Get([]byte(key))
						_ = v
						return nil
					})
					latencies = append(latencies, time.Since(start))
					logMemory(b, i+1, 100, &peak)
				}
				iterElapsed := time.Since(iterStart)
				b.ReportMetric(mmapMiB, "mmap_MiB")
				b.ReportMetric(mmapMiB, "total_disk_MiB")
				b.ReportMetric(float64(itemCount)/iterElapsed.Seconds(), "reads/sec")
				b.ReportMetric(float64(itemCount)*float64(itemSize)/(1024*1024)/iterElapsed.Seconds(), "read_MiB/s")
				b.ReportMetric(float64(peak)/(1024*1024), "peak_heap_MiB")
				b.ReportMetric(float64(itemCount), "total_items")
				b.Logf("Total: %d items read in %v", itemCount, iterElapsed)
				reportLatencyPercentiles(b, latencies)
			}
		})
	}
}
