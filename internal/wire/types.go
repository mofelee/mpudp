package wire

// SessionID identifies a logical Session independently of a UDP five-tuple.
// The all-zero value is reserved and is never valid on the wire.
type SessionID [16]byte

// Header contains the fields common to every decoded packet.
type Header struct {
	Type      PacketType
	SessionID SessionID
}

// Handshake is the body shared by HELLO and HELLO_ACK.
type Handshake struct {
	DataShards    uint8
	ParityShards  uint8
	MaxUDPPayload uint16
}

// DataShard is one authenticated Reed-Solomon shard.
//
// A decoded Payload aliases the datagram passed to DecodeAuthenticated. The
// caller must retain that datagram for as long as Payload is used and must copy
// Payload, after authentication, before retaining it in longer-lived state.
type DataShard struct {
	PacketID       uint64
	DataShards     uint8
	ParityShards   uint8
	ShardIndex     uint8
	OriginalLength uint32
	Payload        []byte
}

// Probe is the body shared by PING and PONG. Timestamp is opaque to the peer
// and must be echoed unchanged in PONG.
type Probe struct {
	Token     uint64
	Timestamp uint64
}

// Message is a value discriminated union. Header.Type selects exactly one of
// Handshake, DataShard, or Probe; CLOSE has no body.
type Message struct {
	Header    Header
	Handshake Handshake
	DataShard DataShard
	Probe     Probe
}

// NewHello constructs a validated HELLO message.
func NewHello(sessionID SessionID, dataShards, parityShards uint8, maxUDPPayload uint16) (Message, error) {
	return newHandshake(TypeHello, sessionID, dataShards, parityShards, maxUDPPayload)
}

// NewHelloAck constructs a validated HELLO_ACK message.
func NewHelloAck(sessionID SessionID, dataShards, parityShards uint8, maxUDPPayload uint16) (Message, error) {
	return newHandshake(TypeHelloAck, sessionID, dataShards, parityShards, maxUDPPayload)
}

func newHandshake(packetType PacketType, sessionID SessionID, dataShards, parityShards uint8, maxUDPPayload uint16) (Message, error) {
	message := Message{
		Header: Header{Type: packetType, SessionID: sessionID},
		Handshake: Handshake{
			DataShards:    dataShards,
			ParityShards:  parityShards,
			MaxUDPPayload: maxUDPPayload,
		},
	}
	if _, err := EncodedLen(message); err != nil {
		return Message{}, err
	}
	return message, nil
}

// NewDataShard constructs a validated DATA_SHARD message. Payload is borrowed
// for the lifetime of the Message and is not copied by this constructor. An
// empty Datagram is represented by OriginalLength zero and one zero shard byte.
func NewDataShard(sessionID SessionID, packetID uint64, dataShards, parityShards, shardIndex uint8, originalLength uint32, payload []byte) (Message, error) {
	message := Message{
		Header: Header{Type: TypeDataShard, SessionID: sessionID},
		DataShard: DataShard{
			PacketID:       packetID,
			DataShards:     dataShards,
			ParityShards:   parityShards,
			ShardIndex:     shardIndex,
			OriginalLength: originalLength,
			Payload:        payload,
		},
	}
	if _, err := EncodedLen(message); err != nil {
		return Message{}, err
	}
	return message, nil
}

// NewPing constructs a validated PING message.
func NewPing(sessionID SessionID, token, timestamp uint64) (Message, error) {
	return newProbe(TypePing, sessionID, token, timestamp)
}

// NewPong constructs a validated PONG message.
func NewPong(sessionID SessionID, token, timestamp uint64) (Message, error) {
	return newProbe(TypePong, sessionID, token, timestamp)
}

func newProbe(packetType PacketType, sessionID SessionID, token, timestamp uint64) (Message, error) {
	message := Message{
		Header: Header{Type: packetType, SessionID: sessionID},
		Probe:  Probe{Token: token, Timestamp: timestamp},
	}
	if _, err := EncodedLen(message); err != nil {
		return Message{}, err
	}
	return message, nil
}

// NewClose constructs a validated, bodyless CLOSE message.
func NewClose(sessionID SessionID) (Message, error) {
	message := Message{Header: Header{Type: TypeClose, SessionID: sessionID}}
	if _, err := EncodedLen(message); err != nil {
		return Message{}, err
	}
	return message, nil
}
