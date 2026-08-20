// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

func testBBoltOpts() bbolt.Options {
	return bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	}
}

func newTestClient(t *testing.T, segmentSize int64) *queueClient {
	t.Helper()
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, segmentSize, 3, testBBoltOpts())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close(context.Background()))
	})
	return client
}

func TestBasicSetGetDelete(t *testing.T) {
	client := newTestClient(t, defaultSegmentSize)
	ctx := context.Background()

	// Set and Get
	require.NoError(t, client.Set(ctx, "hello", []byte("world")))
	val, err := client.Get(ctx, "hello")
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), val)

	// Delete
	require.NoError(t, client.Delete(ctx, "hello"))
	val, err = client.Get(ctx, "hello")
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestNumericKeyRouting(t *testing.T) {
	client := newTestClient(t, defaultSegmentSize)
	ctx := context.Background()

	// Numeric keys are items — stored in write segment
	for i := range 100 {
		key := strconv.FormatUint(uint64(i), 10)
		require.NoError(t, client.Set(ctx, key, []byte(fmt.Sprintf("value-%d", i))))
	}

	// Read them back
	for i := range 100 {
		key := strconv.FormatUint(uint64(i), 10)
		val, err := client.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), val)
	}
}

func TestMetadataKeyRouting(t *testing.T) {
	client := newTestClient(t, defaultSegmentSize)
	ctx := context.Background()

	// Non-numeric key like "qmv0" goes to write segment
	require.NoError(t, client.Set(ctx, "qmv0", []byte("metadata-blob")))
	val, err := client.Get(ctx, "qmv0")
	require.NoError(t, err)
	assert.Equal(t, []byte("metadata-blob"), val)
}

func TestBatchAtomicOffer(t *testing.T) {
	client := newTestClient(t, defaultSegmentSize)
	ctx := context.Background()

	// Simulate persistent queue Offer: Batch(Set("qmv0", meta), Set("<idx>", payload))
	err := client.Batch(ctx,
		storage.SetOperation("qmv0", []byte("meta-v1")),
		storage.SetOperation("0", []byte("payload-0")),
	)
	require.NoError(t, err)

	// Verify both written
	val, err := client.Get(ctx, "qmv0")
	require.NoError(t, err)
	assert.Equal(t, []byte("meta-v1"), val)

	val, err = client.Get(ctx, "0")
	require.NoError(t, err)
	assert.Equal(t, []byte("payload-0"), val)
}

func TestBatchReadDispatch(t *testing.T) {
	client := newTestClient(t, defaultSegmentSize)
	ctx := context.Background()

	// Setup: write some items
	require.NoError(t, client.Set(ctx, "0", []byte("payload-0")))
	require.NoError(t, client.Set(ctx, "qmv0", []byte("meta-v0")))

	// Simulate Read/Dispatch: Batch(Set("qmv0", meta), Get("<idx>"))
	getOp := storage.GetOperation("0")
	err := client.Batch(ctx,
		storage.SetOperation("qmv0", []byte("meta-v1")),
		getOp,
	)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload-0"), getOp.Value)
}

func TestBatchAckDone(t *testing.T) {
	client := newTestClient(t, defaultSegmentSize)
	ctx := context.Background()

	// Setup
	require.NoError(t, client.Set(ctx, "0", []byte("payload-0")))
	require.NoError(t, client.Set(ctx, "qmv0", []byte("meta-v0")))

	// Simulate Ack: Batch(Set("qmv0", meta), Delete("<idx>"))
	err := client.Batch(ctx,
		storage.SetOperation("qmv0", []byte("meta-v2")),
		storage.DeleteOperation("0"),
	)
	require.NoError(t, err)

	// Item is gone
	val, err := client.Get(ctx, "0")
	require.NoError(t, err)
	assert.Nil(t, val)

	// Metadata updated
	val, err = client.Get(ctx, "qmv0")
	require.NoError(t, err)
	assert.Equal(t, []byte("meta-v2"), val)
}

func TestSegmentRolling(t *testing.T) {
	// Use a small segment size (64KB) to force rolling with fewer items.
	segSize := int64(64 * 1024)
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, segSize, 4, testBBoltOpts())
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	ctx := context.Background()

	// Write enough data to trigger multiple rolls (~8KB per item × 50 items = 400KB > 64KB).
	payload := make([]byte, 8*1024) // 8KB per item
	for i := range 50 {
		key := strconv.FormatUint(uint64(i), 10)
		require.NoError(t, client.Batch(ctx,
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("meta-%d", i))),
			storage.SetOperation(key, payload),
		))
	}

	// Verify multiple segment files exist.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	segCount := 0
	for _, e := range entries {
		if _, ok := parseSegmentStartIndex(e.Name()); ok {
			segCount++
		}
	}
	assert.Greater(t, segCount, 1, "expected multiple segment files after rolling")

	// Verify all data is still readable.
	for i := range 50 {
		key := strconv.FormatUint(uint64(i), 10)
		val, err := client.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, payload, val, "item %d mismatch", i)
	}
}

func TestSegmentGCOnAck(t *testing.T) {
	segSize := int64(64 * 1024)
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, segSize, 4, testBBoltOpts())
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	ctx := context.Background()

	// Write items to create multiple segments (8KB × 50 items = 400KB > 64KB).
	payload := make([]byte, 8*1024)
	for i := range 50 {
		key := strconv.FormatUint(uint64(i), 10)
		require.NoError(t, client.Set(ctx, key, payload))
	}

	// Count initial segments.
	countSegments := func() int {
		entries, _ := os.ReadDir(dir)
		count := 0
		for _, e := range entries {
			if _, ok := parseSegmentStartIndex(e.Name()); ok {
				count++
			}
		}
		return count
	}
	initialCount := countSegments()
	assert.Greater(t, initialCount, 1)

	// Delete all items — should trigger GC of old segments.
	for i := range 50 {
		key := strconv.FormatUint(uint64(i), 10)
		require.NoError(t, client.Delete(ctx, key))
	}

	finalCount := countSegments()
	assert.Less(t, finalCount, initialCount, "expected segments to be GC'd after deleting all items")
	// The write segment should always remain.
	assert.GreaterOrEqual(t, finalCount, 1)
}

func TestMetadataAlwaysInWriteSegment(t *testing.T) {
	segSize := int64(64 * 1024)
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, segSize, 4, testBBoltOpts())
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	ctx := context.Background()

	// Write items + metadata causing a roll (8KB × 50 items > 64KB).
	payload := make([]byte, 8*1024)
	for i := range 50 {
		require.NoError(t, client.Batch(ctx,
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("meta-%d", i))),
			storage.SetOperation(strconv.FormatUint(uint64(i), 10), payload),
		))
	}

	// The metadata should be readable (it's in the write segment).
	val, err := client.Get(ctx, "qmv0")
	require.NoError(t, err)
	assert.Equal(t, []byte("meta-49"), val)

	// Verify it's in the write segment by checking that the write segment's
	// bbolt has the "qmv0" key.
	client.mu.Lock()
	ws := client.registry.writeSegment()
	var found bool
	_ = ws.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}
		if b.Get([]byte("qmv0")) != nil {
			found = true
		}
		return nil
	})
	client.mu.Unlock()
	assert.True(t, found, "qmv0 must be in the write segment")
}

func TestPersistentQueuePattern(t *testing.T) {
	segSize := int64(8192)
	dir := t.TempDir()
	ctx := context.Background()

	// Phase 1: Fill queue
	client, err := newQueueClient(zap.NewNop(), dir, segSize, 3, testBBoltOpts())
	require.NoError(t, err)

	itemCount := 500
	payload := make([]byte, 128)
	for i := range itemCount {
		require.NoError(t, client.Batch(ctx,
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("wi=%d", i+1))),
			storage.SetOperation(strconv.FormatUint(uint64(i), 10), payload),
		))
	}
	require.NoError(t, client.Close(ctx))

	// Phase 2: Restart — only 2-3 segment files should be opened.
	client, err = newQueueClient(zap.NewNop(), dir, segSize, 3, testBBoltOpts())
	require.NoError(t, err)

	client.mu.Lock()
	openCount := len(client.registry.openList)
	client.mu.Unlock()
	assert.LessOrEqual(t, openCount, 3, "at most max_open_segments should be open after restart")

	// Phase 3: Consume all items (read + ack pattern).
	for i := range itemCount {
		key := strconv.FormatUint(uint64(i), 10)
		getOp := storage.GetOperation(key)
		require.NoError(t, client.Batch(ctx,
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("ri=%d", i+1))),
			getOp,
		))
		assert.Equal(t, payload, getOp.Value, "item %d", i)

		// Ack
		require.NoError(t, client.Batch(ctx,
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("ack=%d", i))),
			storage.DeleteOperation(key),
		))
	}

	// Verify most segments were GC'd.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	segCount := 0
	for _, e := range entries {
		if _, ok := parseSegmentStartIndex(e.Name()); ok {
			segCount++
		}
	}
	assert.Equal(t, 1, segCount, "only write segment should remain after consuming all items")

	require.NoError(t, client.Close(ctx))
}

func TestCrashRecovery(t *testing.T) {
	segSize := int64(8192)
	dir := t.TempDir()
	ctx := context.Background()

	// Write items and simulate crash (drop without Close).
	client, err := newQueueClient(zap.NewNop(), dir, segSize, 3, testBBoltOpts())
	require.NoError(t, err)

	payload := []byte("crash-test-payload")
	for i := range 50 {
		require.NoError(t, client.Batch(ctx,
			storage.SetOperation("qmv0", []byte(fmt.Sprintf("wi=%d", i+1))),
			storage.SetOperation(strconv.FormatUint(uint64(i), 10), payload),
		))
	}
	// Simulate crash: close bbolt files directly without proper cleanup.
	client.mu.Lock()
	_ = client.registry.closeAll()
	client.mu.Unlock()

	// Reopen — should recover cleanly.
	client2, err := newQueueClient(zap.NewNop(), dir, segSize, 3, testBBoltOpts())
	require.NoError(t, err)
	defer func() { require.NoError(t, client2.Close(ctx)) }()

	// All items should be readable.
	for i := range 50 {
		key := strconv.FormatUint(uint64(i), 10)
		val, err := client2.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, payload, val, "item %d lost after crash", i)
	}

	// Metadata should be intact.
	meta, err := client2.Get(ctx, "qmv0")
	require.NoError(t, err)
	assert.Equal(t, []byte("wi=50"), meta)
}

func TestWalkAllSegments(t *testing.T) {
	segSize := int64(64 * 1024)
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, segSize, 4, testBBoltOpts())
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	ctx := context.Background()

	// Write items spanning multiple segments (8KB × 30 = 240KB > 64KB).
	payload := make([]byte, 8*1024)
	for i := range 30 {
		require.NoError(t, client.Set(ctx, strconv.FormatUint(uint64(i), 10), payload))
	}
	require.NoError(t, client.Set(ctx, "qmv0", []byte("meta")))

	// Walk and collect all keys.
	var keys []string
	err = client.Walk(ctx, func(key string, _ []byte) ([]*storage.Operation, error) {
		keys = append(keys, key)
		return nil, nil
	})
	require.NoError(t, err)

	// Should see all numeric keys + "qmv0".
	assert.Contains(t, keys, "qmv0")
	for i := range 30 {
		assert.Contains(t, keys, strconv.FormatUint(uint64(i), 10))
	}
}

func TestSegmentFilename(t *testing.T) {
	tests := []struct {
		index    uint64
		expected string
	}{
		{0, "seg_00000000000000000000.db"},
		{1, "seg_00000000000000000001.db"},
		{1000, "seg_00000000000000001000.db"},
		{18446744073709551615, "seg_18446744073709551615.db"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, segmentFilename(tt.index))
		idx, ok := parseSegmentStartIndex(tt.expected)
		assert.True(t, ok)
		assert.Equal(t, tt.index, idx)
	}
}

func TestParseSegmentStartIndex_Invalid(t *testing.T) {
	invalids := []string{
		"not_a_segment.db",
		"seg_.db",
		"seg_abc.db",
		"seg_00000000000000000000.txt",
		"other_file",
	}
	for _, name := range invalids {
		_, ok := parseSegmentStartIndex(name)
		assert.False(t, ok, "expected %q to be invalid", name)
	}
}

func TestClosedClientReturnsError(t *testing.T) {
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, client.Close(ctx))

	_, err = client.Get(ctx, "key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestMaxOpenSegmentsEnforced(t *testing.T) {
	segSize := int64(64 * 1024)
	maxOpen := 3
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, segSize, maxOpen, testBBoltOpts())
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	ctx := context.Background()

	// Write enough to create many segments (8KB × 50 = 400KB > 64KB → multiple rolls).
	payload := make([]byte, 8*1024)
	for i := range 50 {
		require.NoError(t, client.Set(ctx, strconv.FormatUint(uint64(i), 10), payload))
	}

	// Now read items from various segments — should never exceed maxOpen.
	for i := range 50 {
		_, err := client.Get(ctx, strconv.FormatUint(uint64(i), 10))
		require.NoError(t, err)

		client.mu.Lock()
		openCount := len(client.registry.openList)
		client.mu.Unlock()
		assert.LessOrEqual(t, openCount, maxOpen, "open segments exceeded max at item %d", i)
	}
}

func TestReopenAfterEviction(t *testing.T) {
	segSize := int64(64 * 1024)
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, segSize, 2, testBBoltOpts())
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	ctx := context.Background()

	// Create enough data for multiple segments (8KB × 30 = 240KB > 64KB).
	payload := make([]byte, 8*1024)
	for i := range 30 {
		require.NoError(t, client.Set(ctx, strconv.FormatUint(uint64(i), 10), payload))
	}

	// Reading an old item should work even if its segment was evicted.
	val, err := client.Get(ctx, "0")
	require.NoError(t, err)
	assert.Equal(t, payload, val)

	// Read a newer item.
	val, err = client.Get(ctx, "29")
	require.NoError(t, err)
	assert.Equal(t, payload, val)

	// Read old item again (after its segment was likely evicted again).
	val, err = client.Get(ctx, "0")
	require.NoError(t, err)
	assert.Equal(t, payload, val)
}

func TestEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	client, err := newQueueClient(zap.NewNop(), dir, defaultSegmentSize, 3, testBBoltOpts())
	require.NoError(t, err)

	// Should have exactly one segment file.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	segCount := 0
	for _, e := range entries {
		if _, ok := parseSegmentStartIndex(e.Name()); ok {
			segCount++
		}
	}
	assert.Equal(t, 1, segCount)

	// The initial segment should start at index 0.
	_, exists := parseSegmentStartIndex("seg_00000000000000000000.db")
	assert.True(t, exists)
	_, err = os.Stat(filepath.Join(dir, "seg_00000000000000000000.db"))
	assert.NoError(t, err)

	require.NoError(t, client.Close(context.Background()))
}
