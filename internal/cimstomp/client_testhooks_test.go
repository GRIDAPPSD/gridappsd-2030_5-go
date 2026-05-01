package cimstomp

// Test-only hooks on *Client. Live in a _test.go file so they are NOT
// compiled into the production binary (Dutch H1, Leon L1). They are
// available to any *_test.go file in this package.
//
// Pre-cleanup, equivalent methods existed in client.go and the
// mark-connected variant planted a zero-value *stomp.Conn placeholder so
// the in-Request nil check would not fire. The Request gate has since
// been changed to treat nil c.conn as ErrNotConnected even when
// connected.Load() returns true (Dutch H2 / item C); the placeholder is
// no longer needed and unit tests run with c.conn == nil.

// markConnectedForTest flips the connected flag without standing up a
// real STOMP session. Used by unit tests that drive code paths gated on
// the flag (cancelled-ctx, expired-deadline) without dialing.
func (c *Client) markConnectedForTest() {
	c.connected.Store(true)
}

// unmarkConnectedForTest reverses markConnectedForTest. Tests that
// toggle the flag must restore it so later tests in the same binary do
// not observe a poisoned Client.
func (c *Client) unmarkConnectedForTest() {
	c.connected.Store(false)
}

// tokenForTest exposes the cached token so integration tests can assert
// that Connect bootstrapped successfully.
func (c *Client) tokenForTest() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token
}
