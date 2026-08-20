// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.etcd.io/bbolt"
)

const (
	segmentFilePrefix = "seg_"
	segmentFileSuffix = ".db"
	segmentIndexWidth = 20 // zero-padded decimal digits for uint64
)

var (
	defaultBucket    = []byte("default")
	prefetchDisabled bool
)

// SetNoPrefetch disables the prefetch optimization (for benchmark comparison).
func SetNoPrefetch(v bool) { prefetchDisabled = v }

// segment wraps a single bbolt database file representing one slice of the queue.
type segment struct {
	path       string
	startIndex uint64
	db         *bbolt.DB
	opts       bbolt.Options
	liveKeys   int64
}

func segmentFilename(startIndex uint64) string {
	return fmt.Sprintf("%s%0*d%s", segmentFilePrefix, segmentIndexWidth, startIndex, segmentFileSuffix)
}

func parseSegmentStartIndex(filename string) (uint64, bool) {
	if !strings.HasPrefix(filename, segmentFilePrefix) || !strings.HasSuffix(filename, segmentFileSuffix) {
		return 0, false
	}
	body := filename[len(segmentFilePrefix) : len(filename)-len(segmentFileSuffix)]
	idx, err := strconv.ParseUint(body, 10, 64)
	return idx, err == nil
}

func createSegment(dir string, startIndex uint64, opts bbolt.Options) (*segment, error) {
	path := filepath.Join(dir, segmentFilename(startIndex))
	return openSegment(path, startIndex, opts)
}

func openSegment(path string, startIndex uint64, opts bbolt.Options) (*segment, error) {
	// Prefetch before Open so freelist reconstruction hits cached pages.
	info, _ := os.Stat(path)
	if info != nil {
		prefetchFile(path, info.Size())
	}
	db, err := bbolt.Open(path, 0o600, &opts)
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
	seg := &segment{
		path:       path,
		startIndex: startIndex,
		db:         db,
		opts:       opts,
	}
	seg.liveKeys = seg.countNumericKeys()
	return seg, nil
}

func (s *segment) close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *segment) reopen() error {
	if s.db != nil {
		return nil
	}
	// Prefetch BEFORE bbolt.Open so that freelist reconstruction
	// (NoFreelistSync=true) hits cached pages instead of causing
	// thousands of individual mmap page faults.
	prefetchFile(s.path, s.fileSize())
	db, err := bbolt.Open(s.path, 0o600, &s.opts)
	if err != nil {
		return err
	}
	s.db = db
	s.warmIndex()
	return nil
}


// warmIndex warms only the B-tree structure pages (branch + leaf) without
// touching overflow data pages. This is ~25 pages (100KB) per segment and
// ensures subsequent reads only fault on sequential data pages, enabling
// OS readahead to prefetch multiple items per fault.
func (s *segment) warmIndex() {
	if s.db == nil {
		return
	}
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			// Iterating keys reads leaf pages (where keys are stored inline)
			// without faulting in overflow data pages.
			_ = k[0]
		}
		return nil
	})
}

func (s *segment) isOpen() bool {
	return s.db != nil
}

func (s *segment) fileSize() int64 {
	info, err := os.Stat(s.path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func (s *segment) isEmpty() bool {
	if s.db == nil {
		return false // can't verify if not open; assume not empty
	}
	var hasNumericKey bool
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			if _, err := strconv.ParseUint(string(k), 10, 64); err == nil {
				hasNumericKey = true
				return nil
			}
		}
		return nil
	})
	return !hasNumericKey
}

func (s *segment) countNumericKeys() int64 {
	if s.db == nil {
		return 0
	}
	var count int64
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			if _, err := strconv.ParseUint(string(k), 10, 64); err == nil {
				count++
			}
		}
		return nil
	})
	return count
}

func (s *segment) readNonNumericKeys() map[string][]byte {
	if s.db == nil {
		return nil
	}
	result := make(map[string][]byte)
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}
		cur := b.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			if _, err := strconv.ParseUint(string(k), 10, 64); err != nil {
				valCopy := make([]byte, len(v))
				copy(valCopy, v)
				result[string(k)] = valCopy
			}
		}
		return nil
	})
	return result
}

func (s *segment) maxNumericKey() (uint64, bool) {
	if s.db == nil {
		return 0, false
	}
	var maxKey uint64
	found := false
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			if idx, err := strconv.ParseUint(string(k), 10, 64); err == nil {
				if !found || idx > maxKey {
					maxKey = idx
					found = true
				}
			}
		}
		return nil
	})
	return maxKey, found
}
