package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
	kcp "github.com/xtaci/kcp-go/v5"
)

type messageConn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}

type framedStream struct {
	net.Conn
	trace *kcpTrace
}

func (c framedStream) Read(b []byte) (int, error) { return io.ReadFull(c.Conn, b) }
func (c framedStream) Write(b []byte) (int, error) {
	if c.trace != nil {
		started := time.Now()
		defer func() { c.trace.applicationWrite(time.Since(started)) }()
	}
	total := 0
	for total < len(b) {
		n, err := c.Conn.Write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}

type datagramConn struct {
	session mpudp.Session
	writer  *admissionWriter
}

func (c datagramConn) Read(b []byte) (int, error) {
	p, err := c.session.ReadPacket()
	if err != nil {
		return 0, err
	}
	if len(p) > len(b) {
		return 0, io.ErrShortBuffer
	}
	return copy(b, p), nil
}
func (c datagramConn) Write(b []byte) (int, error) {
	var err error
	if c.writer != nil {
		err = c.writer.writePacket(context.Background(), b)
	} else {
		err = c.session.WritePacket(b)
	}
	if err != nil {
		return 0, err
	}
	return len(b), nil
}
func (c datagramConn) Close() error {
	if c.writer != nil {
		c.writer.cancel()
	}
	return c.session.Close()
}

type udpConn struct {
	*net.UDPConn
	remote    *net.UDPAddr
	connected bool
	buffer    []byte
}

func (c *udpConn) Read(b []byte) (int, error) {
	for {
		n, addr, err := c.UDPConn.ReadFromUDP(c.buffer)
		if err != nil {
			return 0, err
		}
		if c.remote == nil {
			c.remote = addr
		}
		if addr.String() != c.remote.String() {
			continue
		}
		if n > len(b) {
			return 0, io.ErrShortBuffer
		}
		return copy(b, c.buffer[:n]), nil
	}
}

func (c *udpConn) Write(b []byte) (int, error) {
	if c.connected {
		return c.UDPConn.Write(b)
	}
	return c.UDPConn.WriteToUDP(b, c.remote)
}

// KCP must see transient MPUDP send failures as packet loss. Returning such
// failures to kcp-go permanently stops its output loop; count each one instead.
type packetConn struct {
	s      mpudp.Session
	writer *admissionWriter
	drops  atomic.Uint64
	trace  *kcpTrace
}

var virtualRemote = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}

func (p *packetConn) ReadFrom(b []byte) (int, net.Addr, error) {
	d, err := p.s.ReadPacket()
	if err != nil {
		return 0, virtualRemote, err
	}
	if len(d) > len(b) {
		return 0, virtualRemote, io.ErrShortBuffer
	}
	if p.trace != nil {
		p.trace.incoming(d, time.Now())
	}
	return copy(b, d), virtualRemote, nil
}
func (p *packetConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	var started time.Time
	var writeID uint64
	if p.trace != nil {
		started = time.Now()
		writeID = p.trace.outgoing(b, started)
	}
	var err error
	if p.writer != nil {
		err = p.writer.writePacket(context.Background(), b)
	} else {
		err = p.s.WritePacket(b)
	}
	if p.trace != nil {
		p.trace.returned(b, writeID, started, time.Now(), err)
	}
	if err != nil {
		if recoverableSend(err) {
			p.drops.Add(1)
			return len(b), nil
		}
		return 0, err
	}
	return len(b), nil
}
func (p *packetConn) Close() error {
	if p.writer != nil {
		p.writer.cancel()
	}
	return p.s.Close()
}
func (p *packetConn) LocalAddr() net.Addr              { return virtualRemote }
func (p *packetConn) SetDeadline(time.Time) error      { return errors.New("use KCP session deadline") }
func (p *packetConn) SetReadDeadline(time.Time) error  { return errors.New("use KCP session deadline") }
func (p *packetConn) SetWriteDeadline(time.Time) error { return errors.New("use KCP session deadline") }

func recoverableSend(err error) bool {
	return errors.Is(err, mpudp.ErrNotReady) || errors.Is(err, mpudp.ErrNoAvailablePaths) ||
		errors.Is(err, mpudp.ErrPartialSend) || errors.Is(err, mpudp.ErrAllSendsFailed)
}

type transports struct {
	conns          []messageConn
	peer           *mpudp.Peer
	kcp            []*kcp.UDPSession
	kcpTraces      []*kcpTrace
	adapters       []*packetConn
	writers        []*admissionWriter
	listener       io.Closer
	sockets        []io.Closer
	paths          int
	configMetadata any
}

func (t *transports) close() {
	t.stopAdmissions()
	for _, c := range t.conns {
		_ = c.Close()
	}
	if t.listener != nil {
		_ = t.listener.Close()
	}
	for _, c := range t.sockets {
		_ = c.Close()
	}
	if t.peer != nil {
		_ = t.peer.Close()
	}
}

func openTransports(o options) (*transports, error) {
	t := &transports{paths: 1}
	if o.Protocol != "mpudp" && o.Protocol != "kcp-mpudp" {
		return t, nil
	}
	b, err := os.ReadFile(o.Config)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(b)
	if err != nil {
		return nil, err
	}
	if o.Payload > cfg.Limits.MaxDatagramSize && o.Protocol == "mpudp" {
		return nil, fmt.Errorf("payload exceeds local max_datagram_size %d", cfg.Limits.MaxDatagramSize)
	}
	p, err := mpudp.NewPeer(cfg)
	if err != nil {
		return nil, err
	}
	t.peer = p
	t.paths = len(cfg.Carriers)
	t.configMetadata = mpudpConfigMetadata(cfg)
	// Reflection keeps this probe buildable against the v0.1.0 regression SHA,
	// before the optional diagnostics API existed.
	m := reflect.ValueOf(p).MethodByName("SetDiagnosticsEnabled")
	if m.IsValid() {
		m.Call([]reflect.Value{reflect.ValueOf(o.Diagnostics)})
	}
	return t, nil
}

func (t *transports) addKCP(c *kcp.UDPSession, o options, trace *kcpTrace) (messageConn, error) {
	c.SetStreamMode(true)
	c.SetWindowSize(o.KCPWindow, o.KCPWindow)
	if !c.SetMtu(o.KCPMTU) {
		_ = c.Close()
		return nil, errors.New("KCP MTU rejected")
	}
	c.SetNoDelay(1, 10, 2, 1)
	c.SetACKNoDelay(o.ACKNoDelay)
	if o.Diagnostics && trace == nil {
		trace = newKCPTrace(false)
	}
	t.kcp = append(t.kcp, c)
	t.kcpTraces = append(t.kcpTraces, trace)
	return framedStream{Conn: c, trace: trace}, nil
}

func (t *transports) wrapSession(s mpudp.Session, o options) (messageConn, error) {
	var w *admissionWriter
	if _, ok := s.(localFlusher); ok {
		w = newAdmissionWriter(s)
		t.writers = append(t.writers, w)
	}
	if o.Protocol == "mpudp" {
		return datagramConn{session: s, writer: w}, nil
	}
	p := &packetConn{s: s, writer: w}
	if o.Diagnostics {
		p.trace = newKCPTrace(true)
	}
	c, err := kcp.NewConn4(42, virtualRemote, nil, 0, 0, true, p)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	t.adapters = append(t.adapters, p)
	return t.addKCP(c, o, p.trace)
}

func flowAddress(address string, index int) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 || n+index > 65535 {
		return "", errors.New("invalid flow port range")
	}
	return net.JoinHostPort(host, strconv.Itoa(n+index)), nil
}

func (t *transports) listen(o options) (func(int) (messageConn, error), error) {
	switch o.Protocol {
	case "tcp":
		l, err := net.Listen("tcp", o.Address)
		if err != nil {
			return nil, err
		}
		t.listener = l
		return func(int) (messageConn, error) {
			_ = l.(*net.TCPListener).SetDeadline(time.Now().Add(30 * time.Second))
			c, err := l.Accept()
			if err != nil {
				return nil, err
			}
			return framedStream{Conn: c}, nil
		}, nil
	case "kcp":
		a, err := net.ResolveUDPAddr("udp", o.Address)
		if err != nil {
			return nil, err
		}
		p, err := net.ListenUDP("udp", a)
		if err != nil {
			return nil, err
		}
		t.sockets = append(t.sockets, p)
		if err := configureNativePMTU(p); err != nil {
			return nil, err
		}
		l, err := kcp.ServeConn(nil, 0, 0, p)
		if err != nil {
			return nil, err
		}
		t.listener = l
		return func(int) (messageConn, error) {
			_ = l.SetReadDeadline(time.Now().Add(30 * time.Second))
			c, err := l.AcceptKCP()
			if err != nil {
				return nil, err
			}
			return t.addKCP(c, o, nil)
		}, nil
	case "udp":
		for i := 0; i < o.Flows; i++ {
			address, err := flowAddress(o.Address, i)
			if err != nil {
				return nil, err
			}
			a, err := net.ResolveUDPAddr("udp", address)
			if err != nil {
				return nil, err
			}
			c, err := net.ListenUDP("udp", a)
			if err != nil {
				return nil, err
			}
			if err := configureNativePMTU(c); err != nil {
				_ = c.Close()
				return nil, err
			}
			t.conns = append(t.conns, &udpConn{UDPConn: c, buffer: make([]byte, o.Payload+1)})
		}
		return func(i int) (messageConn, error) { return t.conns[i], nil }, nil
	default:
		l, err := t.peer.Listener()
		if err != nil {
			return nil, err
		}
		return func(int) (messageConn, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s, err := l.Accept(ctx)
			if err != nil {
				return nil, err
			}
			return t.wrapSession(s, o)
		}, nil
	}
}

func (t *transports) dial(o options, index int) (messageConn, error) {
	switch o.Protocol {
	case "tcp":
		d := net.Dialer{Timeout: 15 * time.Second}
		if o.Bind != "" {
			d.LocalAddr = &net.TCPAddr{IP: net.ParseIP(o.Bind)}
		}
		c, err := d.Dial("tcp", o.Address)
		if err != nil {
			return nil, err
		}
		return framedStream{Conn: c}, nil
	case "udp", "kcp":
		address := o.Address
		if o.Protocol == "udp" {
			var err error
			address, err = flowAddress(address, index)
			if err != nil {
				return nil, err
			}
		}
		raddr, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			return nil, err
		}
		laddr := &net.UDPAddr{}
		if o.Bind != "" {
			laddr.IP = net.ParseIP(o.Bind)
		}
		if o.Protocol == "udp" {
			c, err := net.DialUDP("udp", laddr, raddr)
			if err != nil {
				return nil, err
			}
			if err := configureNativePMTU(c); err != nil {
				_ = c.Close()
				return nil, err
			}
			return &udpConn{UDPConn: c, remote: raddr, connected: true, buffer: make([]byte, o.Payload+1)}, nil
		}
		p, err := net.ListenUDP("udp", laddr)
		if err != nil {
			return nil, err
		}
		if err := configureNativePMTU(p); err != nil {
			_ = p.Close()
			return nil, err
		}
		c, err := kcp.NewConn4(uint32(o.Nonce)+uint32(index), raddr, nil, 0, 0, true, p)
		if err != nil {
			_ = p.Close()
			return nil, err
		}
		return t.addKCP(c, o, nil)
	default:
		s, err := t.peer.NewSession()
		if err != nil {
			return nil, err
		}
		return t.wrapSession(s, o)
	}
}
