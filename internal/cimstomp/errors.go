package cimstomp

import "errors"

// Sentinel errors returned by the cimstomp package. Both Client and
// Publisher report not-connected via ErrNotConnected so callers can use
// errors.Is across the two types.
var (
	// ErrNotConnected is returned when Request or Publish is called before
	// Connect or after Close.
	ErrNotConnected = errors.New("cimstomp: not connected")

	// ErrRequestTimeout is returned when the request context deadline
	// expires before the broker delivers a response on the per-request
	// reply-to queue.
	ErrRequestTimeout = errors.New("cimstomp: request timeout")

	// ErrClosed is returned when Connect is called on a Client that has
	// already been closed. Clients are single-shot: to reuse a lifecycle,
	// construct a new one via NewClient. The guard is enforced in
	// Client.Connect; declaration lives here so both Client and any future
	// Publisher consumers can reference the same sentinel.
	ErrClosed = errors.New("cimstomp: client closed")
)
