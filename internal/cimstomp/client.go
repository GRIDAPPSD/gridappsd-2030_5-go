package cimstomp

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-stomp/stomp/v3"
)

// Sentinel errors returned by Client.
var (
	// ErrNotConnected is returned when Request is called before Connect or
	// after Close.
	ErrNotConnected = errors.New("cimstomp: client not connected")

	// ErrRequestTimeout is returned when the request context deadline expires
	// before the broker delivers a response on the per-request reply-to queue.
	ErrRequestTimeout = errors.New("cimstomp: request timeout")
)

// tokenTopic is the GridAPPS-D auth-token bootstrap destination. The broker
// is expected to reply to the SEND with a single frame whose body is the
// auth token string.
const tokenTopic = "/topic/pnnl.goss.token.topic"

// gossHasSubjectHeader and gossSubjectHeader are the GridAPPS-D-specific
// headers that every request SEND must carry. They are not part of the
// STOMP spec; without them the broker rejects or filters our SENDs.
// See: plans/plan-1-design/research-stomp-cim-catalog.md sections 2 and 7.
const (
	gossHasSubjectHeader = "GOSS_HAS_SUBJECT"
	gossSubjectHeader    = "GOSS_SUBJECT"
	replyToHeader        = "reply-to"
)

// heartbeat is the STOMP heartbeat interval in both directions. Matches the
// existing Publisher.
const heartbeat = 10 * time.Second

// Client is a STOMP request/response client for the GridAPPS-D message bus.
//
// At Connect, Client dials STOMP and bootstraps a GridAPPS-D auth token by
// sending a base64-encoded `user:password` to /topic/pnnl.goss.token.topic
// and reading the broker's single-frame reply. The token is cached for the
// lifetime of the Client.
//
// Request sends a body to the given destination with two mandatory
// GridAPPS-D headers attached: `GOSS_HAS_SUBJECT: True` and
// `GOSS_SUBJECT: <token>`. These headers are not in the STOMP spec; they
// are required by the GridAPPS-D broker and are documented only in
// gridappsd-python's goss.py. The reply-to is a per-request
// /temp-queue/response.<ts>; the broker correlates by destination, no
// `correlation-id` header is set.
//
// Concurrency: Client is safe for use from multiple goroutines, but
// Request serializes via an internal mutex so that v0 issues a single
// in-flight request at a time. This is sufficient for the bridge's
// startup CIM queries and periodic refresh; lifting the lock is a
// future change once a real second-consumer exists.
//
// Lifecycle: NewClient allocates; Connect opens the TCP/STOMP session and
// fetches the token; Close disconnects. Close is idempotent. There is no
// auto-reconnect in v0; callers needing reconnect must call Close then
// NewClient + Connect again. See GAGO-012.
type Client struct {
	cfg STOMPConfig

	mu        sync.Mutex
	conn      *stomp.Conn
	token     string
	connected atomic.Bool
	closed    atomic.Bool
}

// NewClient allocates a Client. It does not perform any I/O.
func NewClient(cfg STOMPConfig) *Client {
	return &Client{cfg: cfg}
}

// Connect dials the STOMP broker and fetches the GridAPPS-D auth token.
// It is safe to call only once per Client. Calling Connect after Close
// is undefined.
func (c *Client) Connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// go-stomp v3.1.5's DialWithContext calls net.Dial (not net.DialContext),
	// so a ctx deadline is ignored at the TCP layer. Dial ourselves with
	// net.DialContext to honor ctx, then hand the live conn to
	// stomp.ConnectWithContext which observes ctx for the STOMP handshake.
	var dialer net.Dialer
	tcp, err := dialer.DialContext(ctx, "tcp", c.cfg.Address)
	if err != nil {
		return fmt.Errorf("cimstomp.Client: tcp dial %s: %w", c.cfg.Address, err)
	}

	conn, err := stomp.ConnectWithContext(ctx, tcp,
		stomp.ConnOpt.Login(c.cfg.User, c.cfg.Password),
		stomp.ConnOpt.HeartBeat(heartbeat, heartbeat),
	)
	if err != nil {
		_ = tcp.Close()
		return fmt.Errorf("cimstomp.Client: stomp connect %s: %w", c.cfg.Address, err)
	}

	token, err := fetchAuthToken(ctx, conn, c.cfg.User, c.cfg.Password)
	if err != nil {
		_ = conn.Disconnect()
		return fmt.Errorf("cimstomp.Client: fetch auth token: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.token = token
	c.mu.Unlock()
	c.connected.Store(true)

	return nil
}

// Close disconnects the STOMP session. It is idempotent: calling Close on a
// never-connected or already-closed Client returns nil.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.connected.Store(false)

	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	if err := conn.Disconnect(); err != nil {
		return fmt.Errorf("cimstomp.Client: disconnect: %w", err)
	}
	return nil
}

// Request sends body to destination and waits for a single response frame
// on a per-request /temp-queue/response.<ts> reply-to. The destination may
// be the bare GridAPPS-D form (`goss.gridappsd...`); Request prepends
// `/queue/` if no /queue/, /topic/, or /temp-queue/ prefix is present.
//
// The returned bytes are the raw response body; callers parse JSON.
//
// Errors:
//   - ErrNotConnected if called before Connect or after Close.
//   - ErrRequestTimeout if ctx deadline expires before a response arrives.
//   - context.Canceled if ctx is cancelled mid-flight.
//   - wrapped broker errors otherwise.
func (c *Client) Request(ctx context.Context, destination string, body []byte) ([]byte, error) {
	if !c.connected.Load() {
		return nil, ErrNotConnected
	}
	if err := ctx.Err(); err != nil {
		return nil, mapCtxErr(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Re-check after taking the lock; a concurrent Close may have raced.
	if c.conn == nil {
		return nil, ErrNotConnected
	}

	replyTo := newTempReplyDest()
	dest := normalizeDestination(destination)

	sub, err := c.conn.Subscribe(replyTo, stomp.AckAuto)
	if err != nil {
		return nil, fmt.Errorf("cimstomp.Client: subscribe %s: %w", replyTo, err)
	}
	// Always tear down the subscription before returning. The broker will
	// drop the corresponding /temp-queue/... destination once unsubscribed.
	defer func() {
		_ = sub.Unsubscribe()
	}()

	// Send the request. NOTE: deliberately no `correlation-id` header. The
	// broker correlates via the per-request /temp-queue/... reply-to;
	// adding a correlation-id would be redundant and is not what the
	// GridAPPS-D platform expects. See research-stomp-cim-catalog.md
	// section 7, item 7.
	err = c.conn.Send(dest, "application/json", body,
		stomp.SendOpt.Header(replyToHeader, replyTo),
		stomp.SendOpt.Header(gossHasSubjectHeader, "True"),
		stomp.SendOpt.Header(gossSubjectHeader, c.token),
	)
	if err != nil {
		return nil, fmt.Errorf("cimstomp.Client: send to %s: %w", dest, err)
	}

	// Wait for either the response frame or context cancellation. The
	// subscription channel is the goroutine-safe exit path; closing the
	// subscription via Unsubscribe in the deferred call drains it.
	select {
	case <-ctx.Done():
		return nil, mapCtxErr(ctx.Err())

	case msg, ok := <-sub.C:
		if !ok || msg == nil {
			return nil, fmt.Errorf("cimstomp.Client: subscription closed before response")
		}
		if msg.Err != nil {
			return nil, fmt.Errorf("cimstomp.Client: response error: %w", msg.Err)
		}
		// Copy the body; the underlying frame may be reused.
		out := make([]byte, len(msg.Body))
		copy(out, msg.Body)
		return out, nil
	}
}

// fetchAuthToken implements the GridAPPS-D auth-token bootstrap. It
// subscribes to a fresh /queue/temp.token_resp.<user>.<ts>, sends
// base64(user:password) to /topic/pnnl.goss.token.topic with a reply-to
// header, waits for the single reply frame, and returns its body as the
// token. ctx bounds the wait.
//
// A regular /queue/ destination is used (not /temp-queue/) to match the
// Python upstream's bootstrap convention (gridappsd-python goss.py
// _make_connection). The GridAPPS-D platform's token responder sends the
// token back to the exact destination string carried in reply-to. With
// /temp-queue/ the broker's header-rewriting can vary across versions
// and connections; a regular queue with a unique name is unambiguous.
func fetchAuthToken(ctx context.Context, conn *stomp.Conn, user, password string) (string, error) {
	replyTo := newTokenReplyDest(user)
	sub, err := conn.Subscribe(replyTo, stomp.AckAuto)
	if err != nil {
		return "", fmt.Errorf("subscribe %s: %w", replyTo, err)
	}
	defer func() {
		_ = sub.Unsubscribe()
	}()

	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
	err = conn.Send(tokenTopic, "text/plain", []byte(auth),
		stomp.SendOpt.Header(replyToHeader, replyTo),
	)
	if err != nil {
		return "", fmt.Errorf("send to %s: %w", tokenTopic, err)
	}

	select {
	case <-ctx.Done():
		return "", mapCtxErr(ctx.Err())

	case msg, ok := <-sub.C:
		if !ok || msg == nil {
			return "", fmt.Errorf("token subscription closed before response")
		}
		if msg.Err != nil {
			return "", fmt.Errorf("token response error: %w", msg.Err)
		}
		token := strings.TrimSpace(string(msg.Body))
		if token == "" {
			return "", fmt.Errorf("empty token in broker response")
		}
		return token, nil
	}
}

// normalizeDestination prepends "/queue/" to a bare GridAPPS-D destination.
// Destinations already prefixed with /queue/, /topic/, or /temp-queue/ are
// returned unchanged. go-stomp is stricter than stomp.py about explicit
// prefixes on send (see catalog open question 5).
func normalizeDestination(dest string) string {
	switch {
	case strings.HasPrefix(dest, "/queue/"),
		strings.HasPrefix(dest, "/topic/"),
		strings.HasPrefix(dest, "/temp-queue/"):
		return dest
	default:
		return "/queue/" + dest
	}
}

// tempDestCounter ensures that two calls to newTempReplyDest within the
// same nanosecond still produce distinct destinations.
var tempDestCounter atomic.Uint64

// newTempReplyDest returns a fresh /temp-queue/response.<ts> destination.
// The broker rewrites SUBSCRIBE on /temp-queue/X into a per-connection
// real temporary destination and rewrites reply-to headers to match. The
// suffix is timestamp-based to ease debugging when frames are tcpdumped.
func newTempReplyDest() string {
	n := tempDestCounter.Add(1)
	return fmt.Sprintf("/temp-queue/response.%d.%d", time.Now().UnixNano(), n)
}

// newTokenReplyDest returns a fresh regular queue for the token-bootstrap
// reply path, scoped to the given user. Mirrors the Python upstream's
// `temp.token_resp.<user>-<datetime>` convention but with a strictly
// monotonic counter to guarantee uniqueness across rapid reconnects.
func newTokenReplyDest(user string) string {
	n := tempDestCounter.Add(1)
	return fmt.Sprintf("/queue/temp.token_resp.%s.%d.%d", user, time.Now().UnixNano(), n)
}

// mapCtxErr converts a context error into the package's sentinel error so
// callers can use errors.Is(err, ErrRequestTimeout). Cancellation is
// surfaced verbatim because callers may want to distinguish it.
func mapCtxErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrRequestTimeout
	}
	return err
}

// markConnectedForTest is a hook used by unit tests to drive code paths
// that gate on the connected flag without standing up a real STOMP
// connection. It is not part of the public API.
func (c *Client) markConnectedForTest() {
	c.connected.Store(true)
	// Set a non-nil placeholder so the in-Request nil check does not fire.
	// The placeholder is never used because tests that call this never
	// progress past the ctx pre-check.
	c.mu.Lock()
	c.conn = &stomp.Conn{}
	c.mu.Unlock()
}

// unmarkConnectedForTest reverses markConnectedForTest. Tests that toggle
// the flag must restore it so other tests in the same binary do not
// observe a poisoned Client.
func (c *Client) unmarkConnectedForTest() {
	c.connected.Store(false)
	c.mu.Lock()
	c.conn = nil
	c.mu.Unlock()
}

// tokenForTest exposes the cached token to integration tests so they can
// assert that Connect bootstrapped successfully. Not part of the public
// API.
func (c *Client) tokenForTest() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token
}
