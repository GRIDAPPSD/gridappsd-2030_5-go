package cim

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// queryFn names the four GAGO-010 wrappers in a table-driven shape.
// Each entry binds a wrapper method to (a) the expected SPARQL substrings
// that prove the query template was selected and the feederID substituted,
// and (b) a representative requestType field on the JSON envelope.
type queryCase struct {
	name           string
	call           func(c *Client, ctx context.Context, feederID string) (*QueryDataResult, error)
	wantSubstrings []string // substrings expected in the queryString after feederID substitution
}

func queryCases() []queryCase {
	return []queryCase{
		{
			name: "QuerySolar",
			call: (*Client).QuerySolar,
			wantSubstrings: []string{
				"# Solar - DistSolar",
				"?s r:type c:PhotovoltaicUnit",
				`VALUES ?fdrid {"_FEEDER123"}`,
				"c:PowerElectronicsConnection.PowerElectronicsUnit",
			},
		},
		{
			name: "QueryBattery",
			call: (*Client).QueryBattery,
			wantSubstrings: []string{
				"# Storage - DistStorage",
				"?s r:type c:BatteryUnit",
				`VALUES ?fdrid {"_FEEDER123"}`,
				"c:BatteryUnit.ratedE",
				"c:BatteryUnit.storedE",
				"c:BatteryUnit.batteryState",
			},
		},
		{
			name: "QueryInverter",
			call: (*Client).QueryInverter,
			wantSubstrings: []string{
				`VALUES ?fdrid {"_FEEDER123"}`,
				"c:PowerElectronicsConnection.PowerElectronicsUnit",
				"c:PowerElectronicsConnection.ratedS",
				"?pec c:IdentifiedObject.mRID ?pecid",
			},
		},
		{
			name: "QueryAllDERGroups",
			call: (*Client).QueryAllDERGroups,
			wantSubstrings: []string{
				"#get all EndDeviceGroup",
				`VALUES ?fdrid {"_FEEDER123"}`,
				"?q1 a c:EndDeviceGroup",
				"c:EndDeviceGroup.EndDevice",
				"c:DERFunction",
			},
		},
	}
}

// okEnvelope is the empty-but-valid SPARQL response shape.
const okEnvelope = `{"data":{"head":{"vars":[]},"results":{"bindings":[]}},"responseComplete":true,"id":"x"}`

func TestSPARQLQueriesEnvelope(t *testing.T) {
	t.Parallel()

	for _, tc := range queryCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mr := &mockRequester{resp: []byte(okEnvelope)}
			c := NewClient(mr)

			if _, err := tc.call(c, context.Background(), "_FEEDER123"); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}

			if mr.gotDestination != RequestPowergridModel {
				t.Errorf("destination = %q, want %q", mr.gotDestination, RequestPowergridModel)
			}
			body := decodeBody(t, mr.gotBody)
			if body["requestType"] != "QUERY" {
				t.Errorf(`body["requestType"] = %v, want "QUERY"`, body["requestType"])
			}
			if body["resultFormat"] != "JSON" {
				t.Errorf(`body["resultFormat"] = %v, want "JSON"`, body["resultFormat"])
			}
			qs, ok := body["queryString"].(string)
			if !ok {
				t.Fatalf(`body["queryString"] missing or not a string: %v`, body["queryString"])
			}
			for _, want := range tc.wantSubstrings {
				if !strings.Contains(qs, want) {
					t.Errorf("queryString missing %q\nfull body:\n%s", want, qs)
				}
			}
		})
	}
}

func TestSPARQLQueriesParseRoundTrip(t *testing.T) {
	t.Parallel()

	// Each query is asserted to return its bindings unchanged. v0 wrappers
	// do not project; the catalog defers typed projection to bridge code.
	const payload = `{
		"data":{
			"head":{"vars":["name","bus"]},
			"results":{"bindings":[
				{"name":{"type":"literal","value":"PV1"},"bus":{"value":"BusA"}},
				{"name":{"type":"literal","value":"PV2"},"bus":{"value":"BusB"}}
			]}
		},
		"responseComplete":true,
		"id":"abc"
	}`

	for _, tc := range queryCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mr := &mockRequester{resp: []byte(payload)}
			c := NewClient(mr)

			res, err := tc.call(c, context.Background(), "_FEEDER123")
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := res.Head.Vars; len(got) != 2 || got[0] != "name" || got[1] != "bus" {
				t.Errorf("Head.Vars = %v, want [name bus]", got)
			}
			if len(res.Results.Bindings) != 2 {
				t.Fatalf("len(Bindings) = %d, want 2", len(res.Results.Bindings))
			}
			if v := res.Results.Bindings[0]["name"].Value; v != "PV1" {
				t.Errorf("first name binding = %q, want PV1", v)
			}
			if v := res.Results.Bindings[1]["bus"].Value; v != "BusB" {
				t.Errorf("second bus binding = %q, want BusB", v)
			}
		})
	}
}

func TestSPARQLQueriesInvalidFeederID(t *testing.T) {
	t.Parallel()

	bad := []struct {
		name string
		id   string
	}{
		{"empty", ""},
		{"double-quote", `bad"id`},
		{"angle-open", "bad<id"},
		{"angle-close", "bad>id"},
		{"newline", "bad\nid"},
		{"carriage-return", "bad\rid"},
		{"backslash", `bad\id`},
	}

	for _, b := range bad {
		b := b
		for _, tc := range queryCases() {
			tc := tc
			t.Run(tc.name+"/"+b.name, func(t *testing.T) {
				t.Parallel()

				mr := &mockRequester{resp: []byte(okEnvelope)}
				c := NewClient(mr)

				_, err := tc.call(c, context.Background(), b.id)
				if !errors.Is(err, ErrInvalidFeederID) {
					t.Errorf("err = %v, want ErrInvalidFeederID", err)
				}
				if mr.gotDestination != "" {
					t.Errorf("Requester should not be invoked for invalid feederID; destination = %q", mr.gotDestination)
				}
			})
		}
	}
}

func TestSPARQLQueriesContextCancelled(t *testing.T) {
	t.Parallel()

	for _, tc := range queryCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			mr := &mockRequester{err: context.Canceled}
			c := NewClient(mr)

			_, err := tc.call(c, ctx, "_FEEDER123")
			if !errors.Is(err, context.Canceled) {
				t.Errorf("err = %v, want context.Canceled", err)
			}
		})
	}
}

func TestSPARQLQueriesRequesterError(t *testing.T) {
	t.Parallel()

	for _, tc := range queryCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want := errors.New("transport down")
			mr := &mockRequester{err: want}
			c := NewClient(mr)

			_, err := tc.call(c, context.Background(), "_FEEDER123")
			if !errors.Is(err, want) {
				t.Errorf("err = %v, want wrapping %v", err, want)
			}
		})
	}
}

func TestSPARQLQueriesIncompleteResponse(t *testing.T) {
	t.Parallel()

	for _, tc := range queryCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mr := &mockRequester{resp: []byte(`{"data":{"head":{"vars":[]},"results":{"bindings":[]}},"responseComplete":false}`)}
			c := NewClient(mr)

			_, err := tc.call(c, context.Background(), "_FEEDER123")
			if !errors.Is(err, ErrIncompleteResponse) {
				t.Errorf("err = %v, want ErrIncompleteResponse", err)
			}
		})
	}
}
