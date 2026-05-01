package cimstomp

import (
	"encoding/json"
	"testing"
)

// parsedPayload is used to verify JSON structure in tests.
type parsedPayload struct {
	MRID   string `json:"mRID"`
	Values []struct {
		V  float64 `json:"v"`
		TS int64   `json:"ts"`
		Q  string  `json:"q"`
	} `json:"values"`
}

func TestFormatPayload_SingleSample(t *testing.T) {
	msg := &PointMessage{
		Topic: "/topic/pmu.PMU-001.voltage.a.magnitude",
		MRID:  "PMU-001_VA_MAG",
		Samples: []Sample{
			{Value: 120.5, Timestamp: 1711300000000000000, Quality: "GOOD"},
		},
	}

	payload := formatPayload(msg)

	var p parsedPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		t.Fatalf("invalid JSON: %v\npayload: %s", err, payload)
	}

	if p.MRID != "PMU-001_VA_MAG" {
		t.Errorf("mRID = %q, want PMU-001_VA_MAG", p.MRID)
	}
	if len(p.Values) != 1 {
		t.Fatalf("values length = %d, want 1", len(p.Values))
	}
	if p.Values[0].V != 120.5 {
		t.Errorf("v = %f, want 120.5", p.Values[0].V)
	}
	if p.Values[0].TS != 1711300000000000000 {
		t.Errorf("ts = %d, want 1711300000000000000", p.Values[0].TS)
	}
	if p.Values[0].Q != "GOOD" {
		t.Errorf("q = %q, want GOOD", p.Values[0].Q)
	}
}

func TestFormatPayload_MultipleSamples(t *testing.T) {
	msg := &PointMessage{
		Topic: "/topic/pmu.PMU-001.voltage.a.magnitude",
		MRID:  "PMU-001_VA_MAG",
		Samples: []Sample{
			{Value: 120.1, Timestamp: 1000000000, Quality: "GOOD"},
			{Value: 120.2, Timestamp: 1016666666, Quality: "GOOD"},
			{Value: 120.3, Timestamp: 1033333333, Quality: "SUSPECT"},
		},
	}

	payload := formatPayload(msg)

	var p parsedPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		t.Fatalf("invalid JSON: %v\npayload: %s", err, payload)
	}

	if len(p.Values) != 3 {
		t.Fatalf("values length = %d, want 3", len(p.Values))
	}

	// Verify ordering and values
	expected := []struct {
		v  float64
		ts int64
		q  string
	}{
		{120.1, 1000000000, "GOOD"},
		{120.2, 1016666666, "GOOD"},
		{120.3, 1033333333, "SUSPECT"},
	}

	for i, want := range expected {
		got := p.Values[i]
		if got.V != want.v {
			t.Errorf("values[%d].v = %f, want %f", i, got.V, want.v)
		}
		if got.TS != want.ts {
			t.Errorf("values[%d].ts = %d, want %d", i, got.TS, want.ts)
		}
		if got.Q != want.q {
			t.Errorf("values[%d].q = %q, want %q", i, got.Q, want.q)
		}
	}
}

func TestFormatPayload_EmptySamples(t *testing.T) {
	msg := &PointMessage{
		Topic:   "/topic/pmu.PMU-001.voltage",
		MRID:    "PMU-001_VA",
		Samples: []Sample{},
	}

	payload := formatPayload(msg)

	var p parsedPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		t.Fatalf("invalid JSON: %v\npayload: %s", err, payload)
	}

	if len(p.Values) != 0 {
		t.Errorf("values length = %d, want 0", len(p.Values))
	}
}

func BenchmarkFormatPayload(b *testing.B) {
	msg := &PointMessage{
		Topic: "/topic/pmu.PMU-001.voltage.a.magnitude",
		MRID:  "PMU-001_VA_MAG",
		Samples: []Sample{
			{Value: 120.1, Timestamp: 1711300000000000000, Quality: "GOOD"},
			{Value: 120.2, Timestamp: 1711300016666666666, Quality: "GOOD"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		formatPayload(msg)
	}
}
