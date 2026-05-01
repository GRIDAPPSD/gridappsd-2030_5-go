package cimstomp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// These tests cover the deterministic, broker-free parts of cimstomp.Client:
// queue-prefix normalization, header construction, sentinel errors, the
// not-connected guard, and Close idempotency. Integration with a live STOMP
// broker is covered in client_integration_test.go behind the `integration`
// build tag.

func TestNormalizeDestination(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"bare goss form gets queue prefix",
			"goss.gridappsd.process.request.data.powergridmodel",
			"/queue/goss.gridappsd.process.request.data.powergridmodel"},
		{"already prefixed queue form is preserved",
			"/queue/goss.gridappsd.process.request.config",
			"/queue/goss.gridappsd.process.request.config"},
		{"topic destination is preserved",
			"/topic/pnnl.goss.token.topic",
			"/topic/pnnl.goss.token.topic"},
		{"temp-queue destination is preserved",
			"/temp-queue/response.123",
			"/temp-queue/response.123"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeDestination(tc.in)
			if got != tc.want {
				t.Errorf("normalizeDestination(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNewTempReplyDest_Format(t *testing.T) {
	dest := newTempReplyDest()
	if !strings.HasPrefix(dest, "/temp-queue/response.") {
		t.Fatalf("temp reply dest %q does not start with /temp-queue/response.", dest)
	}
	suffix := strings.TrimPrefix(dest, "/temp-queue/response.")
	if suffix == "" {
		t.Fatalf("temp reply dest %q has empty suffix", dest)
	}
}

func TestNewTempReplyDest_Unique(t *testing.T) {
	// Two consecutive calls must produce different destinations so the broker
	// gives each request its own correlation queue.
	a := newTempReplyDest()
	b := newTempReplyDest()
	if a == b {
		t.Fatalf("two calls to newTempReplyDest returned the same value: %q", a)
	}
}

func TestRequest_NotConnected(t *testing.T) {
	c := NewClient(STOMPConfig{Address: "tcp://127.0.0.1:1", User: "u", Password: "p"})

	_, err := c.Request(context.Background(), "/queue/foo", []byte("{}"))
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Request before Connect: got err = %v, want ErrNotConnected", err)
	}
}

func TestRequest_ContextAlreadyCancelled(t *testing.T) {
	// A cancelled context must short-circuit Request without touching the wire,
	// surfacing ctx.Err().
	c := NewClient(STOMPConfig{Address: "tcp://127.0.0.1:1"})
	c.markConnectedForTest()
	defer c.unmarkConnectedForTest()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Request(ctx, "/queue/foo", []byte("{}"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Request with cancelled ctx: got err = %v, want context.Canceled", err)
	}
}

func TestRequest_DeadlineExceededReturnsTimeout(t *testing.T) {
	// A deadline that has already passed should yield ErrRequestTimeout
	// without dialing.
	c := NewClient(STOMPConfig{Address: "tcp://127.0.0.1:1"})
	c.markConnectedForTest()
	defer c.unmarkConnectedForTest()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := c.Request(ctx, "/queue/foo", []byte("{}"))
	if !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("Request with expired deadline: got err = %v, want ErrRequestTimeout", err)
	}
}

func TestClose_IdempotentBeforeConnect(t *testing.T) {
	c := NewClient(STOMPConfig{Address: "tcp://127.0.0.1:1"})
	if err := c.Close(); err != nil {
		t.Fatalf("first Close on never-connected client: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close on never-connected client: %v", err)
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	if errors.Is(ErrRequestTimeout, ErrNotConnected) {
		t.Errorf("ErrRequestTimeout must not match ErrNotConnected")
	}
	if errors.Is(ErrNotConnected, ErrRequestTimeout) {
		t.Errorf("ErrNotConnected must not match ErrRequestTimeout")
	}
}
