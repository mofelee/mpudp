package handshakev2

import (
	"sync"

	"github.com/mofelee/mpudp/internal/creditv2"
)

// This fixed owner outlives an attempt without retaining its packets, keys,
// engine or adapter. The extra admission claim covers it and its method value.
type retiredStorage struct {
	once              sync.Once
	initial           [MaxInitialReservations]*creditv2.Lease
	receive, metadata *creditv2.Lease
}

func (s *retiredStorage) release() {
	s.once.Do(func() {
		for i, lease := range s.initial {
			s.initial[i] = nil
			lease.Release()
		}
		receive, metadata := s.receive, s.metadata
		s.receive, s.metadata = nil, nil
		receive.Release()
		metadata.Release()
	})
}
