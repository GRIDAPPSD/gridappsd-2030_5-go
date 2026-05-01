package cim

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestBindingAsString(t *testing.T) {
	t.Parallel()

	b := Binding{Type: "literal", Value: "hello"}
	if got := b.AsString(); got != "hello" {
		t.Fatalf("AsString = %q, want %q", got, "hello")
	}
}

func TestBindingAsJSONLDHappyPath(t *testing.T) {
	t.Parallel()

	b := Binding{
		Type:  "literal",
		Value: `{"@id":"_abc123","@type":"PowerElectronicsConnection"}`,
	}

	got, err := b.AsJSONLD()
	if err != nil {
		t.Fatalf("AsJSONLD: %v", err)
	}

	if id, _ := got["@id"].(string); id != "_abc123" {
		t.Errorf(`got["@id"] = %v, want "_abc123"`, got["@id"])
	}
	if ty, _ := got["@type"].(string); ty != "PowerElectronicsConnection" {
		t.Errorf(`got["@type"] = %v, want "PowerElectronicsConnection"`, got["@type"])
	}
}

func TestBindingAsJSONLDInvalidJSON(t *testing.T) {
	t.Parallel()

	b := Binding{Type: "literal", Value: "not json {{{"}
	_, err := b.AsJSONLD()
	if err == nil {
		t.Fatal("AsJSONLD: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "AsJSONLD") {
		t.Errorf("error message %q does not mention AsJSONLD", err.Error())
	}
}

func TestBindingAsJSONLDEmptyValue(t *testing.T) {
	t.Parallel()

	b := Binding{Type: "literal", Value: ""}
	_, err := b.AsJSONLD()
	if err == nil {
		t.Fatal("AsJSONLD on empty value: expected error, got nil")
	}
}

func TestDestinationConstants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"powergrid", RequestPowergridModel, "goss.gridappsd.process.request.data.powergridmodel"},
		{"config", RequestConfig, "goss.gridappsd.process.request.config"},
		{"platform", RequestPlatformStatus, "goss.gridappsd.process.request.status.platform"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Errorf("constant = %q, want %q", tc.got, tc.want)
			}
		})
	}
}

func TestErrIncompleteResponseIsSentinel(t *testing.T) {
	t.Parallel()

	if ErrIncompleteResponse == nil {
		t.Fatal("ErrIncompleteResponse is nil")
	}
	wrapped := fmt.Errorf("context: %w", ErrIncompleteResponse)
	if !errors.Is(wrapped, ErrIncompleteResponse) {
		t.Errorf("errors.Is(wrapped, ErrIncompleteResponse) = false")
	}
}

func TestErrServerErrorIsSentinel(t *testing.T) {
	t.Parallel()

	if ErrServerError == nil {
		t.Fatal("ErrServerError is nil")
	}
	wrapped := fmt.Errorf("context: %w", ErrServerError)
	if !errors.Is(wrapped, ErrServerError) {
		t.Errorf("errors.Is(wrapped, ErrServerError) = false")
	}
}
