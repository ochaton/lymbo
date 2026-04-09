package stats

import (
	"fmt"
	"math"
	"time"
)

type hist struct {
	scale     int32
	count     uint64
	sumSec    float64
	minSec    float64
	maxSec    float64
	zeroCount uint64

	start time.Time

	offset int32
	counts []uint64
}

func newHist(scale int32) *hist {
	return &hist{
		scale:  scale,
		minSec: math.Inf(1),
		maxSec: math.Inf(-1),
		start:  time.Now(),
	}
}

func (h *hist) Snapshot() HistogramSnapshot {
	out := HistogramSnapshot{
		Count:     h.count,
		SumSec:    h.sumSec,
		MinSec:    h.minSec,
		MaxSec:    h.maxSec,
		ZeroCount: h.zeroCount,
		Scale:     h.scale,
		Offset:    h.offset,
		Start:     h.start,
		End:       time.Now(),
	}

	if len(h.counts) > 0 {
		out.Counts = append([]uint64(nil), h.counts...)
	}

	return out
}

func (h *hist) RecordDuration(d time.Duration) {
	x := d.Seconds()
	if x < 0 {
		return // for task duration, ignore negatives
	}

	h.count++
	h.sumSec += x
	if x < h.minSec {
		h.minSec = x
	}
	if x > h.maxSec {
		h.maxSec = x
	}

	if x == 0 {
		h.zeroCount++
		return
	}

	idx := mapToIndex(x, h.scale)
	h.incBucket(idx)
}

func mapToIndex(x float64, scale int32) int32 {
	// index = floor(log2(x) * 2^scale)
	return int32(math.Floor(math.Log2(x) * float64(int64(1)<<scale)))
}

func (h *hist) incBucket(idx int32) {
	if len(h.counts) == 0 {
		h.offset = idx
		h.counts = []uint64{1}
		return
	}

	start := h.offset
	end := h.offset + int32(len(h.counts)) - 1

	switch {
	case idx < start:
		grow := int(start - idx)
		newCounts := make([]uint64, grow+len(h.counts))
		copy(newCounts[grow:], h.counts)
		newCounts[0] = 1
		h.counts = newCounts
		h.offset = idx
	case idx > end:
		grow := int(idx - end)
		h.counts = append(h.counts, make([]uint64, grow)...)
		h.counts[idx-h.offset]++
	default:
		h.counts[idx-h.offset]++
	}
}

type HistogramSnapshot struct {
	Count     uint64
	SumSec    float64
	MinSec    float64
	MaxSec    float64
	ZeroCount uint64

	Scale  int32
	Offset int32
	Counts []uint64

	Start time.Time
	End   time.Time
}

type PromBucketSpan struct {
	Offset int32
	Length uint32
}

type PromNativeHistogram struct {
	Schema        int32
	Count         uint64
	Sum           float64 // seconds
	ZeroThreshold float64
	ZeroCount     uint64

	PositiveSpans  []PromBucketSpan
	PositiveDeltas []int64

	// Optional metadata you may want to preserve.
	Created *time.Time
	Time    time.Time
}

func (s HistogramSnapshot) ToPromNative() (PromNativeHistogram, error) {
	if s.Scale < -4 || s.Scale > 8 {
		return PromNativeHistogram{}, fmt.Errorf("scale %d is outside Prometheus native histogram schema range [-4,8]", s.Scale)
	}

	out := PromNativeHistogram{
		Schema:        s.Scale,
		Count:         s.Count,
		Sum:           s.SumSec,
		ZeroThreshold: 0,
		ZeroCount:     s.ZeroCount,
		Time:          s.End,
	}
	if !s.Start.IsZero() {
		t := s.Start
		out.Created = &t
	}

	if s.Count == 0 && s.ZeroCount == 0 && len(s.Counts) == 0 {
		out.PositiveSpans = []PromBucketSpan{{Offset: 0, Length: 0}}
		return out, nil
	}

	type bucket struct {
		idx   int32
		count uint64
	}
	var nz []bucket
	for i, c := range s.Counts {
		if c == 0 {
			continue
		}
		nz = append(nz, bucket{
			idx:   s.Offset + int32(i) + 1, // +1: lower-bound index -> upper-bound index
			count: c,
		})
	}

	if len(nz) == 0 {
		out.PositiveSpans = []PromBucketSpan{{Offset: 0, Length: 0}}
		return out, nil
	}

	prevAbs := uint64(0)
	prevSpanEnd := int32(0)
	firstSpan := true

	i := 0
	for i < len(nz) {
		startIdx := nz[i].idx

		j := i + 1
		for j < len(nz) && nz[j].idx == nz[j-1].idx+1 {
			j++
		}

		var off int32
		if firstSpan {
			off = startIdx
			firstSpan = false
		} else {
			off = startIdx - (prevSpanEnd + 1)
		}

		out.PositiveSpans = append(out.PositiveSpans, PromBucketSpan{
			Offset: off,
			Length: uint32(j - i),
		})

		for k := i; k < j; k++ {
			delta := int64(nz[k].count) - int64(prevAbs)
			out.PositiveDeltas = append(out.PositiveDeltas, delta)
			prevAbs = nz[k].count
		}

		prevSpanEnd = nz[j-1].idx
		i = j
	}

	return out, nil
}
