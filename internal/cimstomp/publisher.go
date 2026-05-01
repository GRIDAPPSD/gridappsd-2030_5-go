package cimstomp

import (
	"fmt"
	"log"
	"strings"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

// Sample is a single timestamped measurement value.
type Sample struct {
	Value     float64
	Timestamp int64 // nanoseconds since epoch
	Quality   string
}

// PointMessage is a batch of samples for a single measurement point,
// ready to publish to a STOMP topic.
type PointMessage struct {
	Topic   string
	MRID    string
	Samples []Sample
}

// Publisher manages the STOMP connection and publishes CIM messages.
type Publisher struct {
	addr     string
	user     string
	password string
	conn     *stomp.Conn
}

// New creates a Publisher from STOMP config.
func New(cfg STOMPConfig) *Publisher {
	return &Publisher{
		addr:     cfg.Address,
		user:     cfg.User,
		password: cfg.Password,
	}
}

// Connect establishes the STOMP connection. Blocks until connected or error.
func (p *Publisher) Connect() error {
	conn, err := stomp.Dial("tcp", p.addr,
		stomp.ConnOpt.Login(p.user, p.password),
		stomp.ConnOpt.HeartBeat(heartbeat, heartbeat),
		stomp.ConnOpt.Header(frame.ContentType, "application/json"),
	)
	if err != nil {
		return fmt.Errorf("stomp dial %s: %w", p.addr, err)
	}
	p.conn = conn
	log.Printf("STOMP connected to %s", p.addr)
	return nil
}

// Publish sends a PointMessage to its topic. The JSON payload contains
// an array of samples to handle multiple values per timeslice.
//
// Format:
//
//	{"mRID":"...","values":[{"v":1.02,"ts":1711300000000000,"q":"GOOD"},...]}
func (p *Publisher) Publish(msg *PointMessage) error {
	if p.conn == nil {
		return ErrNotConnected
	}

	payload := formatPayload(msg)
	return p.conn.Send(msg.Topic, "application/json", []byte(payload))
}

// Close disconnects from STOMP.
func (p *Publisher) Close() {
	if p.conn != nil {
		p.conn.Disconnect()
		log.Println("STOMP disconnected")
	}
}

// formatPayload builds the JSON string without encoding overhead.
func formatPayload(msg *PointMessage) string {
	var b strings.Builder
	b.Grow(64 + 48*len(msg.Samples))

	b.WriteString(`{"mRID":"`)
	b.WriteString(msg.MRID)
	b.WriteString(`","values":[`)

	for i, s := range msg.Samples {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"v":%g,"ts":%d,"q":"%s"}`, s.Value, s.Timestamp, s.Quality)
	}

	b.WriteString("]}")
	return b.String()
}
