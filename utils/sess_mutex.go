package utils

import "sync"

// A mutex that yields a 'lock session' supporting idempotent unlock
type SessionedMutex struct {
	mtx sync.RWMutex
}

func (s *SessionedMutex) ReadLock() *LockSession {
	s.mtx.RLock()
	return &LockSession{mtx: &s.mtx, readLock: true}
}

func (s *SessionedMutex) Lock() *LockSession {
	s.mtx.Lock()
	return &LockSession{mtx: &s.mtx, readLock: false}
}

type LockSession struct {
	mtx *sync.RWMutex
	readLock bool
}

// Idempotent unlock
func (l *LockSession) Unlock() {
	if l.mtx == nil {
		return
	}

	if l.readLock {
		l.mtx.RUnlock()
	} else {
		l.mtx.Unlock()
	}
	l.mtx = nil
}

