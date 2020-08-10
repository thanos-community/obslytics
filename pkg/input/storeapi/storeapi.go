package storeapi

// Implements input interfaces on top of Thanos Store API endpoints

import (
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-community/obslytics/pkg/input"
)

// Implements input.Series
type StoreSeries struct{ StoreS *storepb.Series }

func (s StoreSeries) Labels() labels.Labels {
	return storepb.LabelsToPromLabels(s.StoreS.Labels)
}

func (s StoreSeries) MinTime() time.Time {
	if len(s.StoreS.Chunks) == 0 {
		return time.Time{}
	}
	return timestamp.Time(s.StoreS.Chunks[0].MinTime)
}

func (s StoreSeries) ChunkIterator() (input.ChunkIterator, error) {
	return NewStoreChunkIterator(s.StoreS.Chunks)
}

// Implements dataframe.ChunkIterator.
type storeChunkIterator struct {
	chunks        []storepb.AggrChunk
	decodedChunks []chunkenc.Chunk
	chunkPos      int
	currentIt     chunkenc.Iterator
}

func NewStoreChunkIterator(chunks []storepb.AggrChunk) (*storeChunkIterator, error) {
	decodedChunks := make([]chunkenc.Chunk, 0, len(chunks))
	for _, c := range chunks {
		ce, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		if err != nil {
			return nil, err
		}
		decodedChunks = append(decodedChunks, ce)
	}
	return &storeChunkIterator{chunks: chunks, decodedChunks: decodedChunks, chunkPos: -1}, nil
}

func (i *storeChunkIterator) At() (int64, float64) {
	return i.currentIt.At()
}

func (i *storeChunkIterator) Err() error {
	return i.currentIt.Err()
}

func (i *storeChunkIterator) Next() bool {
	if !i.ensureCurrentIt() {
		return false
	}
	// try finding next chunk that return true
	for {
		ret := i.currentIt.Next()
		if ret {
			// there is still something in the current iterator
			return true
		}

		if !i.nextChunk() {
			// no more chunks to try
			return false
		}
	}
}

func (i *storeChunkIterator) Seek(t int64) bool {
	if !i.ensureCurrentIt() {
		return false
	}
	for i.chunks[i.chunkPos].MaxTime < t {
		// move until we get to a chunk with MaxTime >= t
		if !i.nextChunk() {
			// no more chunks to try
			return false
		}
	}
	// seek in the chunk that matches MaxTime >= t (or it's the last one)
	return i.currentIt.Seek(t)
}

// Moves the position to the next chunk if available. Returns false if no more
// chunks to try.
func (i *storeChunkIterator) nextChunk() bool {
	if i.chunkPos == len(i.chunks)-1 {
		// no more chunks to try
		return false
	}

	i.chunkPos++
	i.currentIt = i.decodedChunks[i.chunkPos].Iterator(nil)
	return true
}

// At the beginning, the currentIt it not set. This method initializes
// it if missing and possible (not empty chunks).
// Returns true if the iterator is successfully initialized, false otherwise.
func (i *storeChunkIterator) ensureCurrentIt() bool {
	if i.currentIt == nil {
		return i.nextChunk()
	}
	return true
}
