package sessionv2

import (
	"errors"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/aggregationv2"
	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type Controller struct {
	setup                          handshakev2.Setup
	cfg                            Config
	remote                         negotiationv2.Profile
	sendKey, receiveKey            wirev2.Key
	paths                          []pathState
	queue                          *aggregationv2.Queue
	originals                      *reassemblyv2.Receiver
	groups                         map[uint64]*pendingGroup
	groupWindow                    *recvwindow.Window
	groupWindowLease, controlLease *creditv2.Lease
	sendContext, receiveContext    wirev2.EncodingContext
	receiveCodec                   *fecv2.Codec
	context                        controlRetry
	contextPath                    uint16
	contextAcknowledged            bool
	receiveAckSent                 bool
	out                            *aggregationv2.Output
	outNext                        int
	outPaths                       [256]uint16
	outThrough                     uint64
	accepted, completed, forced    uint64
	sticky                         error
	failedFrom                     uint64
	last, retryStorage             time.Time
	started, accepting, closed     bool
	haveTime, busy                 bool
	controlCursor                  int
}

func validBinding(binding handshakev2.Binding) bool {
	if !binding.Local.IsValid() || !binding.Remote.IsValid() {
		return false
	}
	return binding.SocketID != 0 && binding.Local.Port() != 0 && binding.Remote.Port() != 0 && !binding.Local.Addr().IsUnspecified() && !binding.Remote.Addr().IsUnspecified() && !binding.Local.Addr().IsMulticast() && !binding.Remote.Addr().IsMulticast() && binding.Local.Addr() == binding.Local.Addr().Unmap() && binding.Remote.Addr() == binding.Remote.Addr().Unmap()
}

func validateConfig(cfg Config) error {
	if err := cfg.LocalProfile.Validate(); err != nil {
		return err
	}
	p := cfg.LocalProfile
	if p.Protocol != negotiationv2.Datagram || p.Discovery != negotiationv2.Fixed || p.Scope != negotiationv2.SessionBudget || p.Mux != negotiationv2.MuxOff || p.OfferedCaps & ^(negotiationv2.FragmentManifest|negotiationv2.Aggregation) != 0 || p.Repair != (negotiationv2.RepairLimits{}) {
		return ErrUnsupported
	}
	if cfg.Aggregation && p.OfferedCaps&negotiationv2.Aggregation == 0 {
		return ErrUnsupported
	}
	if cfg.FixedPayloadBudget < 512 || cfg.FixedPayloadBudget > p.Payload.SendHardCap || cfg.MaxGroupBytes < 24 || cfg.MaxGroupBytes > fecv2.MaxLogicalBytes || cfg.MaxPendingGroups < 1 || cfg.MaxPendingGroups > 65536 || cfg.MaxPendingGroups > int(p.Datagram.GroupWindow) || cfg.GroupTimeout < 100*time.Millisecond || cfg.GroupTimeout > time.Minute {
		return ErrInvalid
	}
	if cfg.Reassembly.Span != p.Datagram.DatagramWindow || cfg.Reassembly.MaxDatagrams > int(p.Datagram.MaxDatagramAssemblies) || cfg.Reassembly.MaxDatagramBytes != int(p.Datagram.MaxDatagramBytes) || cfg.Reassembly.MaxFragments != int(p.Datagram.MaxFragments) {
		return ErrInvalid
	}
	for id, rate := range cfg.PathRatesBPS {
		if id == 0 || id > p.MaxPaths || rate < 1000 || rate > 1000000000000 {
			return ErrInvalid
		}
	}
	return nil
}

func requiredInitialClaims(cfg Config) ([]creditv2.Claim, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	queueBytes, err := aggregationv2.RequiredInitialBytes(cfg.Queue)
	if err != nil {
		return nil, err
	}
	originalBytes, err := reassemblyv2.RequiredInitialBytes(cfg.Reassembly)
	if err != nil {
		return nil, err
	}
	n := uint64(cfg.LocalProfile.DataShards) + uint64(cfg.LocalProfile.ParityShards)
	// Bounded controller/path records include exact retained control frames.
	// The two codec profiles additionally reserve a conservative matrix budget.
	stateBytes := uint64(unsafe.Sizeof(Controller{})) + uint64(cfg.LocalProfile.MaxPaths)*uint64(unsafe.Sizeof(pathState{})) + 16*n*n + 512*n + 16384
	return []creditv2.Claim{{Bytes: queueBytes}, {Bytes: originalBytes}, {Bytes: 16 * ((uint64(cfg.LocalProfile.Datagram.GroupWindow) + 63) / 64)}, {Bytes: stateBytes}}, nil
}

// New consumes four prepaid initial leases after handshake promotion. It
// constructs storage without emitting packets; Start runs after READY returns.
// On failure it disposes all component storage already constructed.
func New(setup handshakev2.Setup, cfg Config) (*Controller, error) {
	claims, err := requiredInitialClaims(cfg)
	if err != nil {
		return nil, err
	}
	local, remote, err := setup.Contract.Profiles(setup.Role)
	if err != nil || local != cfg.LocalProfile || setup.ID == (wirev2.SessionID{}) || setup.PathID != setup.Contract.BootstrapPathID || !validBinding(setup.Binding) || cfg.BootstrapPath == nil || cfg.Emit == nil || cfg.Entropy == nil || len(setup.Initial) != InitialCount || setup.Scope == nil {
		return nil, ErrInvalid
	}
	if cfg.Aggregation && setup.Contract.ActiveCaps&negotiationv2.Aggregation == 0 {
		return nil, ErrUnsupported
	}
	direction, err := setup.Contract.EffectiveSend(setup.Role, cfg.SendLimits)
	if err != nil {
		return nil, err
	}
	cfg.FixedPayloadBudget = min(cfg.FixedPayloadBudget, direction.MaxUDPPayload)
	cfg.Queue.MaxDatagramBytes = min(cfg.Queue.MaxDatagramBytes, direction.Datagram.MaxDatagramBytes)
	cfg.Queue.MaxFragmentsPerDatagram = min(cfg.Queue.MaxFragmentsPerDatagram, int(direction.Datagram.MaxFragments))
	maxDescriptors := min(cfg.SendLimits.Datagram.MaxDescriptors, direction.Datagram.MaxDescriptors)
	if !cfg.Aggregation {
		maxDescriptors = 1
	}
	context := wirev2.EncodingContext{Epoch: 1, LayoutID: 1, ProtectionID: 1, DataShards: local.DataShards, ParityShards: local.ParityShards, ShardBytes: cfg.FixedPayloadBudget - 94, MaxDescriptors: maxDescriptors, MaxLogicalBytes: min(cfg.MaxGroupBytes, uint32(local.DataShards)*uint32(cfg.FixedPayloadBudget-94))}
	if err := context.Validate(); err != nil {
		return nil, err
	}
	if setup.Role == negotiationv2.Initiator {
		if len(cfg.Carriers) != int(local.MaxPaths) {
			return nil, ErrInvalid
		}
		for i, carrier := range cfg.Carriers {
			if carrier.PathID != uint16(i+1) || !validBinding(carrier.Binding) || carrier.Sender == nil {
				return nil, ErrInvalid
			}
		}
	}
	c := &Controller{setup: setup, cfg: cfg, remote: remote, accepting: true, sendContext: context, contextPath: setup.PathID, groups: make(map[uint64]*pendingGroup)}
	c.sendKey, c.receiveKey = setup.Keys.ClientToServer, setup.Keys.ServerToClient
	if setup.Role == negotiationv2.Responder {
		c.sendKey, c.receiveKey = c.receiveKey, c.sendKey
	}
	if c.sendKey == (wirev2.Key{}) || c.receiveKey == (wirev2.Key{}) {
		return nil, ErrInvalid
	}
	c.controlLease, err = setup.Scope.BindBytes(setup.Initial[InitialControl], claims[InitialControl].Bytes)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			c.Close()
		}
	}()
	c.groupWindowLease, err = setup.Scope.BindBytes(setup.Initial[InitialGroupWindow], claims[InitialGroupWindow].Bytes)
	if err != nil {
		return nil, err
	}
	c.groupWindow, _ = recvwindow.New(local.Datagram.GroupWindow)
	c.queue, err = aggregationv2.NewPrepaid(setup.Scope, cfg.Queue, aggregationv2.Epoch{ID: 1, Parameters: parameters(context, cfg.Queue.MaxDatagramBytes)}, setup.Initial[InitialQueue])
	if err != nil {
		return nil, err
	}
	c.originals, err = reassemblyv2.NewPrepaid(setup.Scope, cfg.Reassembly, setup.Initial[InitialOriginalWindow])
	if err != nil {
		return nil, err
	}
	c.paths = make([]pathState, int(local.MaxPaths))
	for i := range c.paths {
		c.paths[i].id = uint16(i + 1)
		c.paths[i].rate = cfg.PathRatesBPS[uint16(i+1)]
		if c.paths[i].rate == 0 {
			c.paths[i].rate = DefaultPathRateBPS
		}
		if setup.Role == negotiationv2.Initiator {
			c.paths[i].configured = cfg.Carriers[i]
		}
	}
	p := &c.paths[setup.PathID-1]
	p.active, p.generation, p.floor = true, 1, 1
	p.binding, p.sender = setup.Binding, cfg.BootstrapPath
	p.sendBudget, p.receiveBudget, p.sendEpoch, p.receiveEpoch = 512, 512, 1, 1
	c.cfg.Carriers, c.cfg.PathRatesBPS, c.setup.Initial = nil, nil, nil
	failed = false
	return c, nil
}

func parameters(context wirev2.EncodingContext, maxDatagram uint32) fecv2.Parameters {
	return fecv2.Parameters{DataShards: int(context.DataShards), ParityShards: int(context.ParityShards), ShardBytes: int(context.ShardBytes), MaxDescriptors: int(context.MaxDescriptors), MaxLogicalBytes: int(context.MaxLogicalBytes), MaxDatagramBytes: int(maxDatagram)}
}

func (c *Controller) enter(now time.Time) error {
	if c == nil || c.closed || c.setup.Scope.Snapshot().Closed {
		return ErrClosed
	}
	if c.busy {
		return ErrReentrant
	}
	if c.haveTime && now.Before(c.last) {
		return ErrTime
	}
	c.last, c.haveTime, c.busy = now, true, true
	return nil
}

func (c *Controller) leave(result *Result) {
	result.CompletedThrough, result.SendError, result.FailedFrom, result.Ready = c.completed, c.sticky, c.failedFrom, c.ready()
	c.busy = false
}

func (c *Controller) ready() bool {
	if c == nil || c.closed || !c.started || !c.contextAcknowledged {
		return false
	}
	for i := range c.paths {
		if c.eligible(&c.paths[i]) {
			return true
		}
	}
	return false
}

func (c *Controller) eligible(p *pathState) bool {
	return p.active && p.sender != nil && p.sender.Available() && p.sendEpoch == 2 && p.sendBudget >= c.sendContext.ShardBytes+94
}

func (c *Controller) Start(now time.Time) (result Result, err error) {
	if err = c.enter(now); err != nil {
		return result, err
	}
	defer c.leave(&result)
	if c.started {
		return result, nil
	}
	c.started = true
	if err = c.startBudget(&c.paths[c.setup.PathID-1], now); err != nil {
		return result, err
	}
	if c.setup.Role == negotiationv2.Initiator {
		for _, path := range c.paths {
			carrier := path.configured
			if carrier.PathID != c.setup.PathID && carrier.PathID <= c.setup.Contract.MaxPaths {
				if err = c.startJoin(carrier, now); err != nil {
					return result, err
				}
			}
		}
	}
	err = c.drive(now, &result)
	return result, err
}

func (c *Controller) Write(now time.Time, payload []byte) (receipt Receipt, result Result, err error) {
	if err = c.enter(now); err != nil {
		return 0, result, err
	}
	defer c.leave(&result)
	if !c.accepting {
		return 0, result, ErrClosed
	}
	if !c.ready() {
		if c.contextAcknowledged && !c.pendingPathWork() {
			return 0, result, transport.ErrNoAvailablePaths
		}
		return 0, result, ErrNotReady
	}
	id, err := c.queue.Admit(payload, now)
	if err != nil {
		return 0, result, err
	}
	c.accepted = id
	if !c.cfg.Aggregation {
		c.forced = id
	}
	err = c.drive(now, &result)
	return Receipt(id), result, err
}

func (c *Controller) Flush(now time.Time) (fence Fence, result Result, err error) {
	if err = c.enter(now); err != nil {
		return 0, result, err
	}
	defer c.leave(&result)
	fence, c.forced = Fence(c.accepted), c.accepted
	err = c.drive(now, &result)
	if c.sticky != nil {
		err = c.sticky
	}
	return fence, result, err
}

func (c *Controller) Advance(now time.Time) (result Result, err error) {
	if err = c.enter(now); err != nil {
		return result, err
	}
	defer c.leave(&result)
	if !c.started {
		return result, nil
	}
	if err = c.expireControl(now); err != nil {
		return result, err
	}
	c.expireGroups(now)
	_, err = c.originals.Expire(now)
	if err == nil {
		err = c.retryGroups(now, &result)
		if err == nil {
			err = c.drive(now, &result)
		}
	}
	return result, err
}

// FailPath retires authority for one trusted local transport failure. It never
// learns a tuple from a packet, releases admitted originals, or closes another
// Carrier. The runtime must pass the failed immutable socket/tuple binding.
func (c *Controller) FailPath(now time.Time, binding handshakev2.Binding) (result Result, err error) {
	if err = c.enter(now); err != nil {
		return result, err
	}
	defer c.leave(&result)
	if !validBinding(binding) {
		return result, ErrInvalid
	}
	contextPathFailed := false
	for i := range c.paths {
		p := &c.paths[i]
		if p.active && p.binding == binding {
			p.active, p.sender = false, nil
			p.budget, p.replies = controlRetry{}, [2]controlFrame{}
			result.PathsChanged = true
			contextPathFailed = contextPathFailed || p.id == c.contextPath
		}
		if p.join.binding == binding {
			p.join = pathJoin{}
		}
		for j := range p.old {
			if p.old[j].binding == binding {
				p.old[j] = oldRoute{}
			}
		}
	}
	if contextPathFailed && !c.contextAcknowledged {
		c.contextPath = 0
		for i := range c.paths {
			if c.eligible(&c.paths[i]) {
				if err = c.startEncoding(&c.paths[i], now); err != nil {
					return result, err
				}
				break
			}
		}
		if c.contextPath == 0 && !c.pendingPathWork() {
			return result, transport.ErrNoAvailablePaths
		}
	}
	if c.contextAcknowledged && !c.ready() && !c.pendingPathWork() {
		return result, transport.ErrNoAvailablePaths
	}
	err = c.drive(now, &result)
	return result, err
}

// StopAdmissions begins graceful draining without cancelling admitted work.
func (c *Controller) StopAdmissions() {
	if c != nil {
		c.accepting = false
	}
}

// Close clears controller-owned storage before releasing its initial handles.
// Returned deliveries remain independently owned. The handshake owner closes
// the shared credit scope and emits the Session's one best-effort CLOSE.
func (c *Controller) Close() {
	if c == nil || c.closed {
		return
	}
	c.closed, c.accepting = true, false
	if c.queue != nil {
		c.queue.Close()
	}
	if c.originals != nil {
		c.originals.Close()
	}
	for id, group := range c.groups {
		c.releaseGroup(id, group)
	}
	c.groups = nil
	if c.out != nil {
		c.out.Release()
		c.out = nil
	}
	if c.groupWindow != nil {
		c.groupWindow.Close()
	}
	c.paths, c.cfg.Carriers, c.cfg.PathRatesBPS = nil, nil, nil
	c.receiveCodec = nil
	c.context = controlRetry{}
	c.sendKey, c.receiveKey = wirev2.Key{}, wirev2.Key{}
	c.groupWindowLease.Release()
	c.controlLease.Release()
	c.groupWindowLease, c.controlLease = nil, nil
	c.queue, c.originals, c.groupWindow = nil, nil, nil
	c.cfg = Config{}
	c.setup = handshakev2.Setup{}
	c.remote = negotiationv2.Profile{}
	c.sendContext, c.receiveContext = wirev2.EncodingContext{}, wirev2.EncodingContext{}
}

func (c *Controller) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{Closed: true}
	}
	s := Snapshot{Started: c.started, Ready: c.ready(), Accepting: c.accepting, Closed: c.closed, AcceptedThrough: c.accepted, CompletedThrough: c.completed, PendingGroups: len(c.groups), SendError: c.sticky, FailedFrom: c.failedFrom, NextDeadline: c.NextDeadline()}
	if c.queue != nil {
		s.QueuedDatagrams = c.queue.Snapshot().QueuedDatagrams
	}
	for _, p := range c.paths {
		s.Paths = append(s.Paths, PathSnapshot{PathID: p.id, Generation: p.generation, Binding: p.binding, Active: p.active, SendBudget: p.sendBudget, ReceiveBudget: p.receiveBudget, SendEpoch: p.sendEpoch, ReceiveEpoch: p.receiveEpoch, Pending: p.join.pending})
	}
	return s
}

func (c *Controller) drive(now time.Time, result *Result) error {
	c.driveControl(now, result)
	if c.contextAcknowledged && !c.ready() {
		if !c.pendingPathWork() {
			return transport.ErrNoAvailablePaths
		}
	}
	if !c.ready() || now.Before(c.retryStorage) {
		return nil
	}
	err := c.driveData(now, result)
	if errors.Is(err, creditv2.ErrResourceLimit) {
		c.retryStorage = now.Add(time.Millisecond)
		return nil
	}
	return err
}

func (c *Controller) pendingPathWork() bool {
	for i := range c.paths {
		p := &c.paths[i]
		if (p.join.pending && !p.join.committed) || (p.active && p.budget.pending) {
			return true
		}
	}
	return false
}

func (c *Controller) Receive(now time.Time, binding handshakev2.Binding, reply transport.ReplyPath, packet []byte) (result Result, err error) {
	if err = c.enter(now); err != nil {
		return result, err
	}
	defer c.leave(&result)
	if !validBinding(binding) || reply == nil {
		return result, ErrInvalid
	}
	if !c.started {
		return result, ErrNotReady
	}
	envelope, err := wirev2.ParseEnvelope(packet)
	if err != nil {
		return result, err
	}
	if envelope.Header().SessionID != c.setup.ID || envelope.Header().Type.IsHandshake() {
		return result, nil
	}
	authenticated, err := envelope.Authenticate(c.receiveKey)
	if err != nil {
		return result, err
	}
	message, err := wirev2.DecodeEstablished(authenticated)
	if err != nil {
		return result, err
	}
	if message.Route.PathID == 0 || message.Route.PathID > uint32(c.setup.Contract.MaxPaths) {
		return result, ErrProtocol
	}
	p := &c.paths[message.Route.PathID-1]
	if message.Header.Type >= wirev2.TypePathJoin && message.Header.Type <= wirev2.TypePathReady {
		err = c.receiveJoin(p, now, binding, reply, message, &result)
	} else {
		budget, restricted, ok := c.validRoute(p, binding, message.Route, now)
		if !ok && c.allowBudgetRetry(p, binding, message, now) {
			budget, restricted, ok = 512, true, true
		}
		if !ok || len(packet) > int(budget) {
			return result, nil
		}
		if restricted && message.Route.Generation != p.generation && message.Header.Type != wirev2.TypeFECBundle {
			return result, nil
		}
		switch message.Header.Type {
		case wirev2.TypePathBudgetUpdate, wirev2.TypePathBudgetAck:
			err = c.receiveBudget(p, now, message)
		case wirev2.TypeEncodingContext, wirev2.TypeEncodingContextAck:
			err = c.receiveEncoding(p, now, authenticated)
		case wirev2.TypeFECBundle:
			err = c.receiveBundle(now, authenticated, int(budget), restricted, len(packet), &result)
		default:
			err = ErrUnsupported
		}
	}
	if err == nil {
		err = c.drive(now, &result)
	}
	return result, err
}
