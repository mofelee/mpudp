package transport

import (
	"context"
	"errors"
	"testing"
)

func BenchmarkTransportSendAttempt(b *testing.B) {
	benchmarkTransportSend(b, func(ctx context.Context, path ReplyPath, payload []byte) error {
		at, err := SendWithAttempt(ctx, path, payload)
		if at.IsZero() && err == nil {
			return errors.New("native attempt time unavailable")
		}
		return err
	})
}
