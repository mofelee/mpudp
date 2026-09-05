package transport

import (
	"context"
	"time"
)

// SendWithAttempt sends exactly as path.Send and also reports the time immediately
// before its connection write, after write-lock and deadline setup. A zero time
// means no known write attempt: the native path rejected the send beforehand, or
// a custom ReplyPath is used. It is never dispatch or kernel transmit time.
// A nonzero time survives write, short-write, cancellation and deadline-reset
// errors; it does not mean the datagram was successfully sent.
func SendWithAttempt(ctx context.Context, path ReplyPath, payload []byte) (time.Time, error) {
	if path == nil {
		return time.Time{}, invalidArgument("nil reply path")
	}
	// An adapter may override Send while inheriting a native timing method.
	// Exact types prevent bypassing that adapter's normal send behavior.
	switch native := path.(type) {
	case *Carrier:
		return native.SendWithAttempt(ctx, payload)
	case carrierReplyPath:
		return native.SendWithAttempt(ctx, payload)
	case listenerReplyPath:
		return native.SendWithAttempt(ctx, payload)
	}
	return time.Time{}, path.Send(ctx, payload)
}
