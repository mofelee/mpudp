package mpudp

import (
	"errors"
	"unsafe"
)

type v2CleanupJob struct {
	session  *v2Session
	listener *v2Listener
	carriers []runtimeCarrier
	carrier  runtimeCarrier
	index    int
	socket   runtimePacketListener
}

type v2CleanupResult struct {
	session  *v2Session
	listener *v2Listener
	index    int
	err      error
}

type v2CleanupSlot struct {
	jobs    chan v2CleanupJob
	results chan v2CleanupResult
	done    chan struct{}
	busy    bool
}

func v2CleanupWorkerBytes() uint64 {
	return uint64(unsafe.Sizeof(v2CleanupSlot{})) + uint64(unsafe.Sizeof(v2CleanupJob{})) + uint64(unsafe.Sizeof(v2CleanupResult{})) + 4096
}

func (r *v2Peer) startCleanupWorker() {
	r.cleanup = v2CleanupSlot{jobs: make(chan v2CleanupJob, 1), results: make(chan v2CleanupResult, 1), done: make(chan struct{})}
	go func() {
		defer close(r.cleanup.done)
		for job := range r.cleanup.jobs {
			result := v2CleanupResult{session: job.session, listener: job.listener, index: job.index}
			for _, carrier := range job.carriers {
				if carrier != nil {
					result.err = errors.Join(result.err, carrier.Close())
				}
			}
			clear(job.carriers)
			if job.carrier != nil {
				result.err = errors.Join(result.err, job.carrier.Close())
			}
			if job.socket != nil {
				result.err = errors.Join(result.err, job.socket.Close())
			}
			job = v2CleanupJob{}
			r.cleanup.results <- result
			result = v2CleanupResult{}
			select {
			case r.sendWake <- struct{}{}:
			default:
			}
		}
	}()
}

// Waiting cleanup is represented by the Session's existing ownership record;
// it never allocates a job or queue entry for each failed packet or path.
func (r *v2Peer) queueSessionCleanup(s *v2Session) {
	if !s.closed || s.constructing || s.activeSends != 0 || s.cleanupReady || s.cleanupActive || s.releaseStorage == nil {
		return
	}
	if s.controller != nil {
		if err := s.controller.FinalizeClose(); err != nil {
			r.report("MPUDP v2 finalization failed", err)
			return
		}
		s.controller = nil
	}
	s.cleanupReady = true
	r.peer.wakeDriver()
}

func (r *v2Peer) dispatchCleanup() {
	if r.cleanup.busy {
		return
	}
	for s := range r.sessions {
		if !s.cleanupReady {
			continue
		}
		s.cleanupReady, s.cleanupActive = false, true
		job := v2CleanupJob{session: s, carriers: s.carriers, index: -1}
		s.carriers = nil
		r.cleanup.busy = true
		r.cleanup.jobs <- job
		return
	}
	for s := range r.sessions {
		if s.closed || s.constructing || s.cleanupActive {
			continue
		}
		for i, failed := range s.carrierRetiring {
			if !failed || s.carriers[i] == nil {
				continue
			}
			s.cleanupActive = true
			r.cleanup.busy = true
			r.cleanup.jobs <- v2CleanupJob{session: s, carrier: s.carriers[i], index: i}
			return
		}
	}
	l := r.listener
	if l == nil || !l.closed || l.cleanupActive || l.cleanupComplete {
		return
	}
	for s := range r.sessions {
		if s.inbound {
			return
		}
	}
	l.cleanupActive = true
	r.cleanup.busy = true
	job := v2CleanupJob{listener: l, socket: r.peer.listenerSocket, index: -1}
	r.peer.listenerSocket = nil
	r.cleanup.jobs <- job
}

func (r *v2Peer) consumeCleanupCompletion() {
	select {
	case result := <-r.cleanup.results:
		r.cleanup.busy = false
		if l := result.listener; l != nil {
			l.cleanupActive, l.cleanupComplete = false, true
			l.closeErr = mapV2Error(errors.Join(l.closeErr, result.err))
			l.accept = nil
			close(l.cleanupDone)
			return
		}
		s := result.session
		s.cleanupActive = false
		s.closeErr = mapV2Error(errors.Join(s.closeErr, result.err))
		if result.index >= 0 {
			s.carriers[result.index] = nil
			s.carrierRetiring[result.index] = false
			r.queueSessionCleanup(s)
			return
		}
		for _, path := range s.paths {
			delete(r.sockets, path.Binding.SocketID)
		}
		s.paths, s.carrierRetiring = nil, nil
		r.unlinkSendSession(s)
		delete(r.sessions, s)
		if s.inbound && r.listener != nil {
			r.listener.closeErr = errors.Join(r.listener.closeErr, s.closeErr)
		} else if r.closed {
			r.shutdownErr = errors.Join(r.shutdownErr, s.closeErr)
		}
		release := s.releaseStorage
		s.releaseStorage = nil
		release()
		close(s.cleanupDone)
	default:
	}
}

func (r *v2Peer) joinCleanupWorker() {
	close(r.cleanup.jobs)
	<-r.cleanup.done
}
