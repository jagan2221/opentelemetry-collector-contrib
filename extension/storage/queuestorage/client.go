// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

type segmentRegistry struct {
	dir          string
	segments     []*segment
	bboltOpts    bbolt.Options
	segmentSize  int64
	maxOpen      int
	openList     []*segment
	nextWriteIdx uint64
}

type queueClient struct {
	logger   *zap.Logger
	registry *segmentRegistry
	mu       sync.Mutex
	closed   bool
}

var (
	_ storage.Client = (*queueClient)(nil)
	_ storage.Walker = (*queueClient)(nil)
)

func newQueueClient(logger *zap.Logger, dir string, segSize int64, maxOpen int, opts bbolt.Options) (*queueClient, error) {
	reg, err := initRegistry(dir, segSize, maxOpen, opts)
	if err != nil {
		return nil, err
	}
	return &queueClient{logger: logger, registry: reg}, nil
}

func initRegistry(dir string, segmentSize int64, maxOpen int, opts bbolt.Options) (*segmentRegistry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var startIndices []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if idx, ok := parseSegmentStartIndex(entry.Name()); ok {
			startIndices = append(startIndices, idx)
		}
	}
	sort.Slice(startIndices, func(i, j int) bool { return startIndices[i] < startIndices[j] })

	r := &segmentRegistry{
		dir:         dir,
		bboltOpts:   opts,
		segmentSize: segmentSize,
		maxOpen:     maxOpen,
	}

	if len(startIndices) == 0 {
		seg, err := createSegment(dir, 0, opts)
		if err != nil {
			return nil, err
		}
		r.segments = []*segment{seg}
		r.openList = []*segment{seg}
		r.nextWriteIdx = 0
		return r, nil
	}

	// Create closed stubs for all segments except the last (write segment).
	for _, idx := range startIndices[:len(startIndices)-1] {
		path := filepath.Join(dir, segmentFilename(idx))
		r.segments = append(r.segments, &segment{path: path, startIndex: idx, opts: opts})
	}

	// Open only the write segment.
	lastIdx := startIndices[len(startIndices)-1]
	writeSeg, err := openSegment(filepath.Join(dir, segmentFilename(lastIdx)), lastIdx, opts)
	if err != nil {
		return nil, err
	}
	r.segments = append(r.segments, writeSeg)
	r.openList = []*segment{writeSeg}

	if maxKey, found := writeSeg.maxNumericKey(); found {
		r.nextWriteIdx = maxKey + 1
	} else {
		r.nextWriteIdx = lastIdx
	}

	return r, nil
}

func (r *segmentRegistry) writeSegment() *segment {
	return r.segments[len(r.segments)-1]
}

func (r *segmentRegistry) routeRead(keyIdx uint64) *segment {
	lo, hi := 0, len(r.segments)-1
	result := hi
	for lo <= hi {
		mid := (lo + hi) / 2
		if r.segments[mid].startIndex <= keyIdx {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return r.segments[result]
}

func (r *segmentRegistry) ensureOpen(seg *segment) error {
	if seg.isOpen() {
		r.touchLRU(seg)
		return nil
	}
	// Evict if at capacity.
	if len(r.openList) >= r.maxOpen {
		evict := r.lruEvictCandidate()
		if evict != nil {
			if err := evict.close(); err != nil {
				return err
			}
			r.removeFromOpenList(evict)
		}
	}
	if err := seg.reopen(); err != nil {
		return err
	}
	r.openList = append([]*segment{seg}, r.openList...)
	return nil
}

func (r *segmentRegistry) lruEvictCandidate() *segment {
	ws := r.writeSegment()
	for i := len(r.openList) - 1; i >= 0; i-- {
		if r.openList[i] != ws {
			return r.openList[i]
		}
	}
	return nil
}

func (r *segmentRegistry) touchLRU(seg *segment) {
	r.removeFromOpenList(seg)
	r.openList = append([]*segment{seg}, r.openList...)
}

func (r *segmentRegistry) removeFromOpenList(seg *segment) {
	for i, s := range r.openList {
		if s == seg {
			r.openList = append(r.openList[:i], r.openList[i+1:]...)
			return
		}
	}
}

func (r *segmentRegistry) rollIfNeeded() error {
	ws := r.writeSegment()
	if ws.liveKeys == 0 {
		return nil // don't roll an empty segment
	}
	if ws.fileSize() < r.segmentSize {
		return nil
	}

	// Read metadata (non-numeric keys) from current write segment to migrate.
	metaKeys := ws.readNonNumericKeys()

	newSeg, err := createSegment(r.dir, r.nextWriteIdx, r.bboltOpts)
	if err != nil {
		return err
	}

	// Migrate metadata to the new write segment.
	if len(metaKeys) > 0 {
		var ops []*storage.Operation
		for k, v := range metaKeys {
			ops = append(ops, storage.SetOperation(k, v))
		}
		if err := execBatch(newSeg.db, ops); err != nil {
			_ = newSeg.close()
			_ = os.Remove(newSeg.path)
			return err
		}
	}

	r.segments = append(r.segments, newSeg)
	r.openList = append([]*segment{newSeg}, r.openList...)
	// Evict if over capacity.
	for len(r.openList) > r.maxOpen {
		evict := r.lruEvictCandidate()
		if evict == nil {
			break
		}
		_ = evict.close()
		r.removeFromOpenList(evict)
	}
	return nil
}

func (r *segmentRegistry) gcSegment(seg *segment) error {
	if seg == r.writeSegment() {
		return nil
	}
	if seg.liveKeys > 0 {
		return nil
	}
	// Double-check with bbolt scan.
	if seg.isOpen() && !seg.isEmpty() {
		return nil
	}
	r.removeFromOpenList(seg)
	_ = seg.close()
	if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	for i, s := range r.segments {
		if s == seg {
			r.segments = append(r.segments[:i], r.segments[i+1:]...)
			break
		}
	}
	return nil
}

func (r *segmentRegistry) closeAll() error {
	var errs []error
	for _, seg := range r.segments {
		if seg.isOpen() {
			if err := seg.close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// --- storage.Client implementation ---

func (c *queueClient) Get(ctx context.Context, key string) ([]byte, error) {
	op := storage.GetOperation(key)
	if err := c.Batch(ctx, op); err != nil {
		return nil, err
	}
	return op.Value, nil
}

func (c *queueClient) Set(ctx context.Context, key string, value []byte) error {
	return c.Batch(ctx, storage.SetOperation(key, value))
}

func (c *queueClient) Delete(ctx context.Context, key string) error {
	return c.Batch(ctx, storage.DeleteOperation(key))
}

func (c *queueClient) Batch(_ context.Context, ops ...*storage.Operation) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("storage is closed")
	}

	return c.batchLocked(ops...)
}

func (c *queueClient) batchLocked(ops ...*storage.Operation) error {
	if len(ops) == 0 {
		return nil
	}

	r := c.registry

	// Group operations by target segment.
	type segGroup struct {
		seg *segment
		ops []*storage.Operation
	}
	var groups []segGroup
	segIndex := make(map[*segment]int)

	for _, op := range ops {
		target := c.routeOp(op)
		if idx, ok := segIndex[target]; ok {
			groups[idx].ops = append(groups[idx].ops, op)
		} else {
			segIndex[target] = len(groups)
			groups = append(groups, segGroup{seg: target, ops: []*storage.Operation{op}})
		}
	}

	// Execute write-segment group first for atomicity of Offer operations.
	ws := r.writeSegment()
	for i, g := range groups {
		if g.seg == ws && i != 0 {
			groups[0], groups[i] = groups[i], groups[0]
			break
		}
	}

	for _, g := range groups {
		if err := r.ensureOpen(g.seg); err != nil {
			return err
		}
		if err := execBatch(g.seg.db, g.ops); err != nil {
			return err
		}
		// Update liveKeys counters.
		for _, op := range g.ops {
			if _, isNumeric := parseUint64Key(op.Key); !isNumeric {
				continue
			}
			switch op.Type {
			case storage.Set:
				g.seg.liveKeys++
			case storage.Delete:
				g.seg.liveKeys--
				if g.seg.liveKeys < 0 {
					g.seg.liveKeys = 0
				}
			}
		}
	}

	// GC empty non-write segments.
	for _, g := range groups {
		if g.seg != ws && g.seg.liveKeys <= 0 {
			if err := r.gcSegment(g.seg); err != nil {
				c.logger.Warn("segment GC failed", zap.String("path", g.seg.path), zap.Error(err))
			}
		}
	}

	// Roll write segment if it exceeds size limit.
	if err := r.rollIfNeeded(); err != nil {
		c.logger.Warn("segment roll failed", zap.Error(err))
	}

	return nil
}

func (c *queueClient) routeOp(op *storage.Operation) *segment {
	r := c.registry
	switch op.Type {
	case storage.Set:
		// All writes go to the write segment.
		if idx, ok := parseUint64Key(op.Key); ok {
			if idx+1 > r.nextWriteIdx {
				r.nextWriteIdx = idx + 1
			}
		}
		return r.writeSegment()
	case storage.Get, storage.Delete:
		if idx, ok := parseUint64Key(op.Key); ok {
			return r.routeRead(idx)
		}
		return r.writeSegment()
	default:
		return r.writeSegment()
	}
}

// Walk implements storage.Walker — iterates all segments in startIndex order.
func (c *queueClient) Walk(ctx context.Context, fn storage.WalkFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("storage is closed")
	}

	var collectedOps []*storage.Operation

	for _, seg := range c.registry.segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.registry.ensureOpen(seg); err != nil {
			return err
		}

		var stopped bool
		err := seg.db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(defaultBucket)
			if bucket == nil {
				return nil
			}
			cur := bucket.Cursor()
			for k, v := cur.First(); k != nil; k, v = cur.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				valueCopy := make([]byte, len(v))
				copy(valueCopy, v)

				ops, err := fn(string(k), valueCopy)
				if errors.Is(err, storage.SkipAll) {
					collectedOps = append(collectedOps, ops...)
					stopped = true
					return nil
				}
				if err != nil {
					return err
				}
				collectedOps = append(collectedOps, ops...)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if stopped {
			break
		}
	}

	// Apply collected operations.
	return c.batchLocked(collectedOps...)
}

func (c *queueClient) Close(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	return c.registry.closeAll()
}

// --- helpers ---

func execBatch(db *bbolt.DB, ops []*storage.Operation) error {
	return normalizeStorageError(db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		if bucket == nil {
			return errors.New("storage not initialized")
		}
		for _, op := range ops {
			switch op.Type {
			case storage.Get:
				v := bucket.Get([]byte(op.Key))
				if v != nil {
					op.Value = make([]byte, len(v))
					copy(op.Value, v)
				} else {
					op.Value = nil
				}
			case storage.Set:
				if err := bucket.Put([]byte(op.Key), op.Value); err != nil {
					return err
				}
			case storage.Delete:
				if err := bucket.Delete([]byte(op.Key)); err != nil {
					return err
				}
			default:
				return errors.New("wrong operation type")
			}
		}
		return nil
	}))
}

func normalizeStorageError(err error) error {
	if errors.Is(err, berrors.ErrMaxSizeReached) {
		return storage.ErrStorageFull
	}
	return err
}

func parseUint64Key(key string) (uint64, bool) {
	idx, err := strconv.ParseUint(key, 10, 64)
	return idx, err == nil
}
