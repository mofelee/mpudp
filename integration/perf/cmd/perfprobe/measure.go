package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

type sample struct {
	Type   string  `json:"type"`
	Side   string  `json:"side"`
	Role   string  `json:"role"`
	Second int     `json:"second"`
	Steady bool    `json:"steady"`
	Mbps   float64 `json:"mbps"`
	bucket
	Telemetry telemetry `json:"telemetry"`
}

type throughputSample struct {
	Second int     `json:"second"`
	Steady bool    `json:"steady"`
	Mbps   float64 `json:"mbps"`
	bucket
}

type summary struct {
	Type             string    `json:"type"`
	RunID            string    `json:"run_id"`
	Side             string    `json:"side"`
	Role             string    `json:"role"`
	Protocol         string    `json:"protocol"`
	Direction        string    `json:"direction"`
	Flows            int       `json:"flows"`
	PathCount        int       `json:"path_count"`
	Started          time.Time `json:"started_utc"`
	Seconds          int       `json:"seconds"`
	Warmup           int       `json:"warmup_seconds"`
	MessageBytes     int       `json:"message_bytes"`
	Mbps             float64   `json:"mbps"`
	Worst5SecondMbps *float64  `json:"worst_5_second_mbps"`
	bucket
	Samples       []throughputSample `json:"samples,omitempty"`
	EchoRTT       latencySummary     `json:"echo_rtt"`
	SendErrors    uint64             `json:"send_errors"`
	ReadErrors    uint64             `json:"read_errors"`
	ErrorExamples []string           `json:"error_examples,omitempty"`
	Initial       telemetry          `json:"initial"`
	Final         telemetry          `json:"final"`
}

type kcpSessionStats struct {
	Flow      int    `json:"flow"`
	SRTTMS    int32  `json:"srtt_ms"`
	SRTTVarMS int32  `json:"srtt_variation_ms"`
	RTOMS     uint32 `json:"rto_ms"`
}

type processStats struct {
	CPUUserSeconds   float64 `json:"cpu_user_seconds"`
	CPUSystemSeconds float64 `json:"cpu_system_seconds"`
	MaxRSSKiB        int64   `json:"max_rss_kib"`
	HeapAllocBytes   uint64  `json:"heap_alloc_bytes"`
	TotalAllocBytes  uint64  `json:"total_alloc_bytes"`
	Mallocs          uint64  `json:"mallocs"`
	GCCount          uint32  `json:"gc_count"`
	Goroutines       int     `json:"goroutines"`
}

type telemetry struct {
	At                       time.Time         `json:"at_utc"`
	Process                  processStats      `json:"process"`
	MPUDP                    any               `json:"mpudp,omitempty"`
	MPUDPStatisticsAvailable bool              `json:"mpudp_statistics_available"`
	KCP                      *kcp.Snmp         `json:"kcp_snmp,omitempty"`
	KCPTimeoutRetransmits    uint64            `json:"kcp_timeout_retransmits"`
	KCPSessions              []kcpSessionStats `json:"kcp_sessions,omitempty"`
	AdapterWriteDrops        uint64            `json:"adapter_write_drops"`
}

func (t *transports) telemetry() telemetry {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	v := telemetry{At: time.Now().UTC(), Process: processStats{
		CPUUserSeconds:   float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6,
		CPUSystemSeconds: float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6,
		MaxRSSKiB:        usage.Maxrss, HeapAllocBytes: mem.HeapAlloc, TotalAllocBytes: mem.TotalAlloc,
		Mallocs: mem.Mallocs, GCCount: mem.NumGC, Goroutines: runtime.NumGoroutine(),
	}}
	if t.peer != nil {
		m := reflect.ValueOf(t.peer).MethodByName("Statistics")
		if m.IsValid() {
			v.MPUDP = m.Call(nil)[0].Interface()
			v.MPUDPStatisticsAvailable = true
		}
	}
	if len(t.kcp) > 0 {
		v.KCP = kcp.DefaultSnmp.Copy()
		// In pinned kcp-go v5.6.72, LostSegs increments only in the RTO
		// retransmit branch. It is not evidence of actual network loss.
		v.KCPTimeoutRetransmits = v.KCP.LostSegs
		for i, c := range t.kcp {
			v.KCPSessions = append(v.KCPSessions, kcpSessionStats{i, c.GetSRTT(), c.GetSRTTVar(), c.GetRTO()})
		}
	}
	for _, p := range t.adapters {
		v.AdapterWriteDrops += p.drops.Load()
	}
	return v
}

type workerErrors struct {
	mu           sync.Mutex
	examples     []string
	reads, sends atomic.Uint64
}

func (e *workerErrors) add(err error, send bool) {
	if send {
		e.sends.Add(1)
	} else {
		e.reads.Add(1)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.examples) < 8 {
		e.examples = append(e.examples, err.Error())
	}
}

func resultBase(o options, t *transports, start time.Time, role string) summary {
	return summary{Type: "summary", RunID: o.ID, Side: o.Mode, Role: role, Protocol: o.Protocol,
		Direction: o.Direction, Flows: o.Flows, PathCount: t.paths, Started: start.UTC(), Seconds: o.Seconds,
		Warmup: o.Warmup, MessageBytes: o.Payload, Initial: t.telemetry()}
}

func sumBucket(a *bucket, b bucket) {
	a.VerifiedBytes += b.VerifiedBytes
	a.VerifiedPackets += b.VerifiedPackets
	a.CorruptFrames += b.CorruptFrames
	a.DuplicateFrames += b.DuplicateFrames
	a.TooOldFrames += b.TooOldFrames
}

func receive(t *transports, o options, start time.Time) (summary, error) {
	r := resultBase(o, t, start, "receiver")
	counters := make([]*receiverCounters, o.Flows)
	var wg sync.WaitGroup
	var stopped atomic.Bool
	var errs workerErrors
	latency := latencyCounters{sent: uint64(o.Seconds * 5 * o.Flows)}
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() {
		stopped.Store(true)
		stopOnce.Do(func() { close(stop) })
		for _, c := range t.conns {
			_ = c.Close()
		}
		wg.Wait()
	}
	defer stopWorkers()
	probeStart := start.Add(time.Duration(o.Warmup) * time.Second)
	for i, c := range t.conns {
		counter := &receiverCounters{start: start, buckets: make([]bucket, o.Warmup+o.Seconds)}
		counters[i] = counter
		wg.Add(3)
		jobs := make(chan int, 1)
		go func() {
			defer wg.Done()
			defer close(jobs)
			for seq := 0; seq < o.Seconds*5; seq++ {
				due := probeStart.Add(time.Duration(seq) * 200 * time.Millisecond)
				timer := time.NewTimer(time.Until(due))
				select {
				case <-stop:
					timer.Stop()
					return
				case <-timer.C:
				}
				if time.Now().After(due.Add(time.Second)) {
					latency.markQueueMissed()
					continue
				}
				select {
				case jobs <- seq:
				default:
					latency.markQueueMissed()
				}
			}
		}()
		go func(i int, c messageConn) {
			defer wg.Done()
			b := payload(o.Payload, o.Nonce, i)
			for seq := range jobs {
				due := probeStart.Add(time.Duration(seq) * 200 * time.Millisecond)
				if time.Now().After(due.Add(time.Second)) {
					latency.markQueueMissed()
					continue
				}
				stamp(b, kindProbe, uint64(seq), 0)
				if _, err := c.Write(b); err != nil {
					latency.markWriteFailed()
					if !stopped.Load() {
						errs.add(err, true)
					}
				} else {
					latency.markSubmitted()
				}
			}
		}(i, c)
		go func(i int, c messageConn) {
			defer wg.Done()
			want, b := payload(o.Payload, o.Nonce, i), make([]byte, o.Payload)
			var seen sequenceWindow
			probeSeen := make([]bool, o.Seconds*5)
			for {
				n, err := c.Read(b)
				if err != nil {
					if !stopped.Load() {
						errs.add(err, false)
					}
					if errors.Is(err, io.ErrShortBuffer) {
						counter.record(o.Payload, false, false, false)
						continue
					}
					return
				}
				if !validPayload(b[:n], want) {
					counter.record(o.Payload, false, false, false)
					continue
				}
				kind := binary.BigEndian.Uint32(b[20:])
				if kind == kindHello {
					continue
				}
				seq := binary.BigEndian.Uint64(b[24:])
				if kind == kindEcho {
					if seq >= uint64(len(probeSeen)) {
						errs.add(errors.New("invalid echo sequence"), false)
						continue
					}
					if !probeSeen[seq] {
						probeSeen[seq] = true
						latency.observe(time.Since(probeStart.Add(time.Duration(seq) * 200 * time.Millisecond)))
					}
					continue
				}
				if kind != kindData {
					counter.record(o.Payload, false, false, false)
					continue
				}
				unique, old := seen.accept(seq)
				counter.record(o.Payload, true, unique, old)
			}
		}(i, c)
	}
	for second := 1; second <= o.Warmup+o.Seconds; second++ {
		time.Sleep(time.Until(start.Add(time.Duration(second) * time.Second)))
		var b bucket
		for _, c := range counters {
			sumBucket(&b, c.snapshot(second-1))
		}
		s := sample{Type: "sample", Side: o.Mode, Role: "receiver", Second: second - o.Warmup, Steady: second > o.Warmup,
			Mbps: float64(b.VerifiedBytes) * 8 / 1e6, bucket: b, Telemetry: t.telemetry()}
		r.Samples = append(r.Samples, throughputSample{s.Second, s.Steady, s.Mbps, s.bucket})
		if s.Steady {
			sumBucket(&r.bucket, b)
		}
		if err := emit(s); err != nil {
			return r, err
		}
	}
	// Allow the last scheduled request its full deadline. Throughput counters
	// already reject post-window traffic; bulk load continues during this drain.
	time.Sleep(time.Until(start.Add(time.Duration(o.Warmup+o.Seconds+1) * time.Second)))
	stopWorkers()
	r.EchoRTT = latency.snapshot()
	r.Mbps = float64(r.VerifiedBytes) * 8 / float64(o.Seconds) / 1e6
	if o.Seconds >= 5 {
		worst := math.Inf(1)
		for i := o.Warmup; i+5 <= len(r.Samples); i++ {
			var n uint64
			for _, s := range r.Samples[i : i+5] {
				n += s.VerifiedBytes
			}
			worst = math.Min(worst, float64(n)*8/5e6)
		}
		r.Worst5SecondMbps = &worst
	}
	r.SendErrors, r.ReadErrors, r.ErrorExamples = errs.sends.Load(), errs.reads.Load(), errs.examples
	r.Final = t.telemetry()
	return r, emit(r)
}

func send(t *transports, o options, control net.Conn) (summary, error) {
	start := time.Now()
	r := resultBase(o, t, start, "sender")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var stopOnce sync.Once
	var stopped atomic.Bool
	var errs workerErrors
	for i, c := range t.conns {
		wg.Add(2)
		var sendMu sync.Mutex
		go func(i int, c messageConn) {
			defer wg.Done()
			b := payload(o.Payload, o.Nonce, i)
			for seq := uint64(0); ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				stamp(b, kindData, seq, 0)
				sendMu.Lock()
				_, err := c.Write(b)
				sendMu.Unlock()
				if err != nil {
					if stopped.Load() {
						return
					}
					errs.add(err, true)
					if !recoverableSend(err) {
						return
					}
				}
				if o.RateMbps > 0 {
					// Limit each unpaced burst to 64 KiB (or one large message).
					batch := max(1, min(64, 65536/o.Payload))
					if o.RateMbps <= 10 {
						batch = 1
					}
					if (seq+1)%uint64(batch) != 0 {
						continue
					}
					delayNS := float64(seq+1) * float64(o.Payload) * 8 / o.RateMbps * 1000
					if delayNS > float64(math.MaxInt64) {
						errs.add(errors.New("pacing duration exceeds time.Duration bound"), true)
						return
					}
					target := start.Add(time.Duration(delayNS))
					if delay := time.Until(target); delay > 0 {
						timer := time.NewTimer(delay)
						select {
						case <-stop:
							timer.Stop()
							return
						case <-timer.C:
						}
					}
				}
			}
		}(i, c)
		go func(i int, c messageConn) {
			defer wg.Done()
			want, b := payload(o.Payload, o.Nonce, i), make([]byte, o.Payload)
			for {
				n, err := c.Read(b)
				if err != nil {
					if !stopped.Load() {
						errs.add(err, false)
					}
					return
				}
				if !validPayload(b[:n], want) || binary.BigEndian.Uint32(b[20:]) != kindProbe {
					if validPayload(b[:n], want) && binary.BigEndian.Uint32(b[20:]) == kindHello {
						continue
					}
					errs.add(errors.New("invalid echo frame"), false)
					continue
				}
				stamp(b, kindEcho, binary.BigEndian.Uint64(b[24:]), 0)
				sendMu.Lock()
				_, err = c.Write(b)
				sendMu.Unlock()
				if err != nil {
					if !stopped.Load() {
						errs.add(err, true)
					}
					return
				}
			}
		}(i, c)
	}
	stopWorkers := func() {
		stopped.Store(true)
		stopOnce.Do(func() { close(stop) })
		for _, c := range t.conns {
			_ = c.Close()
		}
		wg.Wait()
	}
	defer stopWorkers()
	type response struct {
		message controlMessage
		err     error
	}
	result := make(chan response, 1)
	go func() { m, err := controlRead(control, "result"); result <- response{m, err} }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	second := 0
	for {
		select {
		case <-ticker.C:
			second++
			if err := emit(sample{Type: "sample", Side: o.Mode, Role: "sender", Second: second - o.Warmup, Steady: second > o.Warmup, Telemetry: t.telemetry()}); err != nil {
				return r, err
			}
		case response := <-result:
			if response.err != nil {
				return r, response.err
			}
			if response.message.Summary == nil {
				return r, fmt.Errorf("missing receiver summary")
			}
			stopWorkers()
			r.SendErrors, r.ReadErrors, r.ErrorExamples = errs.sends.Load(), errs.reads.Load(), errs.examples
			r.Final = t.telemetry()
			if err := emit(map[string]any{"type": "remote_summary", "summary": response.message.Summary}); err != nil {
				return r, err
			}
			return r, emit(r)
		}
	}
}
