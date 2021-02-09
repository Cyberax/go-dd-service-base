package utils

import "testing"

func TestSessMutex(t *testing.T) {
	mtx := SessionedMutex{}

	sess := mtx.Lock()
	sess.Unlock()
	sess.Unlock() //Check unlock idempotency

	// Read locks (non-exclusive)
	sess2 := mtx.ReadLock()
	sess3 := mtx.ReadLock()
	sess2.Unlock()
	sess3.Unlock()
}
