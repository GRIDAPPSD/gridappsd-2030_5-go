package cimstomp

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_Valid(t *testing.T) {
	path := writeTemp(t, `{
		"sttp": {"address": "localhost:7175", "filter_expression": "FILTER TOP 20 ActiveMeasurements WHERE True"},
		"stomp": {"address": "localhost:61613", "user": "admin", "password": "admin"},
		"topic": {"prefix": "/topic/pmu", "segments": ["device", "meas_type"]},
		"signal_mapping_file": "mapping.json"
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.STTP.Address != "localhost:7175" {
		t.Errorf("sttp.address = %q, want localhost:7175", cfg.STTP.Address)
	}
	if cfg.STOMP.Address != "localhost:61613" {
		t.Errorf("stomp.address = %q, want localhost:61613", cfg.STOMP.Address)
	}
	if cfg.Topic.Prefix != "/topic/pmu" {
		t.Errorf("topic.prefix = %q, want /topic/pmu", cfg.Topic.Prefix)
	}
	if len(cfg.Topic.Segments) != 2 {
		t.Errorf("topic.segments length = %d, want 2", len(cfg.Topic.Segments))
	}
}

func TestLoad_MissingSTTPAddress(t *testing.T) {
	path := writeTemp(t, `{
		"sttp": {"address": ""},
		"stomp": {"address": "localhost:61613", "user": "a", "password": "b"},
		"topic": {"prefix": "/topic/x", "segments": ["device"]},
		"signal_mapping_file": "m.json"
	}`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing sttp.address")
	}
}

func TestLoad_MissingSTOMPAddress(t *testing.T) {
	path := writeTemp(t, `{
		"sttp": {"address": "localhost:7175"},
		"stomp": {"address": "", "user": "a", "password": "b"},
		"topic": {"prefix": "/topic/x", "segments": ["device"]},
		"signal_mapping_file": "m.json"
	}`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing stomp.address")
	}
}

func TestLoad_MissingTopicPrefix(t *testing.T) {
	path := writeTemp(t, `{
		"sttp": {"address": "localhost:7175"},
		"stomp": {"address": "localhost:61613", "user": "a", "password": "b"},
		"topic": {"prefix": "", "segments": ["device"]},
		"signal_mapping_file": "m.json"
	}`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing topic.prefix")
	}
}

func TestLoad_EmptySegments(t *testing.T) {
	path := writeTemp(t, `{
		"sttp": {"address": "localhost:7175"},
		"stomp": {"address": "localhost:61613", "user": "a", "password": "b"},
		"topic": {"prefix": "/topic/x", "segments": []},
		"signal_mapping_file": "m.json"
	}`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty segments")
	}
}

func TestLoad_InvalidSegment(t *testing.T) {
	path := writeTemp(t, `{
		"sttp": {"address": "localhost:7175"},
		"stomp": {"address": "localhost:61613", "user": "a", "password": "b"},
		"topic": {"prefix": "/topic/x", "segments": ["device", "bogus"]},
		"signal_mapping_file": "m.json"
	}`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid segment 'bogus'")
	}
}

func TestLoad_AllSegments(t *testing.T) {
	path := writeTemp(t, `{
		"sttp": {"address": "localhost:7175"},
		"stomp": {"address": "localhost:61613", "user": "a", "password": "b"},
		"topic": {"prefix": "/topic/pmu", "segments": ["device", "meas_type", "phase", "component"]},
		"signal_mapping_file": "m.json"
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Topic.Segments) != 4 {
		t.Errorf("segments length = %d, want 4", len(cfg.Topic.Segments))
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	path := writeTemp(t, `{invalid json}`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
