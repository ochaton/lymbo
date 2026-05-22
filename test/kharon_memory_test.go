package lymbo_test

import (
	"testing"

	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/store/memory"
)

// memoryStoreFactory creates a memory store for testing
func memoryStoreFactory(t *testing.T) (lymbo.Store, func()) {
	t.Helper()
	store := memory.NewStore()
	return store, func() {
		// No cleanup needed for memory store
	}
}

// TestMemoryStore runs the full test suite against the memory store
func TestMemoryStore(t *testing.T) {
	suite := NewStoreTestSuite(memoryStoreFactory)
	suite.RunAll(t)
}

// TestMemoryFinalizer runs the finalizer test suite against the memory store
func TestMemoryFinalizer(t *testing.T) {
	suite := NewFinalizerTestSuite(memoryStoreFactory)
	suite.RunAll(t)
}

// TestMemoryConcurrent runs the concurrent test suite against the memory store
func TestMemoryConcurrent(t *testing.T) {
	suite := NewConcurrentTestSuite(memoryStoreFactory)
	suite.RunAll(t)
}
