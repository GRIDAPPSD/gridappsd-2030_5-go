package cim

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// mockRequester captures the last (destination, body) pair passed to Request
// and returns a pre-configured response or error. It is the substrate for
// every test in this file; nothing in this package should hit a real broker.
type mockRequester struct {
	gotDestination string
	gotBody        []byte
	gotCtx         context.Context

	resp []byte
	err  error
}

func (m *mockRequester) Request(ctx context.Context, destination string, body []byte) ([]byte, error) {
	m.gotCtx = ctx
	m.gotDestination = destination
	// Copy body so callers cannot mutate the captured bytes after the fact.
	m.gotBody = append([]byte(nil), body...)
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

func decodeBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decoding captured body %s: %v", raw, err)
	}
	return m
}

// ---------- QueryData ----------

func TestQueryDataEnvelope(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{
		resp: []byte(`{"data":{"head":{"vars":[]},"results":{"bindings":[]}},"responseComplete":true,"id":"x"}`),
	}
	c := NewClient(mr)

	const sparql = "SELECT ?s WHERE { ?s ?p ?o }"
	if _, err := c.QueryData(context.Background(), sparql); err != nil {
		t.Fatalf("QueryData: %v", err)
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
	if body["queryString"] != sparql {
		t.Errorf(`body["queryString"] = %v, want %q`, body["queryString"], sparql)
	}
}

func TestQueryDataParse(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{
		"data":{
			"head":{"vars":["name","bus"]},
			"results":{"bindings":[
				{"name":{"type":"literal","value":"Load1"},"bus":{"value":"BusA"}},
				{"name":{"type":"literal","value":"Load2"},"bus":{"value":"BusB"}}
			]}
		},
		"responseComplete":true,
		"id":"abc"
	}`)}
	c := NewClient(mr)

	res, err := c.QueryData(context.Background(), "SELECT *")
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}

	if got := res.Head.Vars; len(got) != 2 || got[0] != "name" || got[1] != "bus" {
		t.Errorf("Head.Vars = %v, want [name bus]", got)
	}
	if len(res.Results.Bindings) != 2 {
		t.Fatalf("len(Bindings) = %d, want 2", len(res.Results.Bindings))
	}
	if v := res.Results.Bindings[0]["name"].Value; v != "Load1" {
		t.Errorf("first name binding = %q, want Load1", v)
	}
	if ty := res.Results.Bindings[0]["name"].Type; ty != "literal" {
		t.Errorf("first name binding type = %q, want literal", ty)
	}
	if v := res.Results.Bindings[1]["bus"].Value; v != "BusB" {
		t.Errorf("second bus binding = %q, want BusB", v)
	}
}

func TestQueryDataIncompleteResponse(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"data":{"head":{"vars":[]},"results":{"bindings":[]}},"responseComplete":false}`)}
	c := NewClient(mr)

	_, err := c.QueryData(context.Background(), "SELECT *")
	if !errors.Is(err, ErrIncompleteResponse) {
		t.Fatalf("err = %v, want ErrIncompleteResponse", err)
	}
}

func TestQueryDataErrorEnvelope(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"error":"something broke","header":{},"message":"bad input"}`)}
	c := NewClient(mr)

	_, err := c.QueryData(context.Background(), "SELECT *")
	if err == nil {
		t.Fatal("expected error envelope to surface")
	}
	if !errors.Is(err, ErrServerError) {
		t.Errorf("err = %v, want errors.Is ErrServerError", err)
	}
	if !strings.Contains(err.Error(), "something broke") {
		t.Errorf("err = %q, want containing 'something broke'", err)
	}
}

func TestQueryDataRequesterError(t *testing.T) {
	t.Parallel()

	want := errors.New("transport down")
	mr := &mockRequester{err: want}
	c := NewClient(mr)

	_, err := c.QueryData(context.Background(), "SELECT *")
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want wrapping %v", err, want)
	}
}

func TestQueryDataContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mr := &mockRequester{err: context.Canceled}
	c := NewClient(mr)

	_, err := c.QueryData(ctx, "SELECT *")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// ---------- QueryModelInfo ----------

func TestQueryModelInfoEnvelope(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"data":{"models":[]}}`)}
	c := NewClient(mr)

	if _, err := c.QueryModelInfo(context.Background()); err != nil {
		t.Fatalf("QueryModelInfo: %v", err)
	}

	if mr.gotDestination != RequestPowergridModel {
		t.Errorf("destination = %q, want %q", mr.gotDestination, RequestPowergridModel)
	}
	body := decodeBody(t, mr.gotBody)
	if body["requestType"] != "QUERY_MODEL_INFO" {
		t.Errorf(`body["requestType"] = %v, want "QUERY_MODEL_INFO"`, body["requestType"])
	}
	if body["resultFormat"] != "JSON" {
		t.Errorf(`body["resultFormat"] = %v, want "JSON"`, body["resultFormat"])
	}
	if _, present := body["queryString"]; present {
		t.Error(`body["queryString"] should not be present for QUERY_MODEL_INFO`)
	}
}

func TestQueryModelInfoParse(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"data":{"models":[
		{"modelId":"_abc","modelName":"ieee123","stationName":"S1","subRegionName":"SR1","regionName":"R1"},
		{"modelId":"_def","modelName":"ieee13","stationName":"S2","subRegionName":"SR2","regionName":"R2"}
	]}}`)}
	c := NewClient(mr)

	res, err := c.QueryModelInfo(context.Background())
	if err != nil {
		t.Fatalf("QueryModelInfo: %v", err)
	}
	if len(res.Models) != 2 {
		t.Fatalf("len(Models) = %d, want 2", len(res.Models))
	}
	if res.Models[0].ModelID != "_abc" {
		t.Errorf("Models[0].ModelID = %q, want _abc", res.Models[0].ModelID)
	}
	if res.Models[0].ModelName != "ieee123" {
		t.Errorf("Models[0].ModelName = %q, want ieee123", res.Models[0].ModelName)
	}
	if res.Models[1].StationName != "S2" {
		t.Errorf("Models[1].StationName = %q, want S2", res.Models[1].StationName)
	}
}

// ---------- QueryObjectDict ----------

func TestQueryObjectDictEnvelope(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"data":{}}`)}
	c := NewClient(mr)

	const modelID = "_xyz"
	const objectType = "PowerElectronicsConnection"
	if _, err := c.QueryObjectDict(context.Background(), modelID, objectType); err != nil {
		t.Fatalf("QueryObjectDict: %v", err)
	}

	if mr.gotDestination != RequestPowergridModel {
		t.Errorf("destination = %q, want %q", mr.gotDestination, RequestPowergridModel)
	}
	body := decodeBody(t, mr.gotBody)
	if body["requestType"] != "QUERY_OBJECT_DICT" {
		t.Errorf(`body["requestType"] = %v, want "QUERY_OBJECT_DICT"`, body["requestType"])
	}
	if body["modelId"] != modelID {
		t.Errorf(`body["modelId"] = %v, want %q`, body["modelId"], modelID)
	}
	if body["objectType"] != objectType {
		t.Errorf(`body["objectType"] = %v, want %q`, body["objectType"], objectType)
	}
}

func TestQueryObjectDictParse(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"data":{"PowerElectronicsConnection":[{"mRID":"_p1","name":"PEC1"}]}}`)}
	c := NewClient(mr)

	res, err := c.QueryObjectDict(context.Background(), "_xyz", "PowerElectronicsConnection")
	if err != nil {
		t.Fatalf("QueryObjectDict: %v", err)
	}
	pecs, ok := res.Data["PowerElectronicsConnection"]
	if !ok {
		t.Fatalf("res.Data missing PowerElectronicsConnection key, got keys: %v", res.Data)
	}
	pecList, ok := pecs.([]any)
	if !ok || len(pecList) != 1 {
		t.Fatalf("PowerElectronicsConnection value = %v, want slice of 1", pecs)
	}
}

// ---------- GetCIMDictionary ----------

func TestGetCIMDictionaryEnvelope(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"data":{"feeders":[]}}`)}
	c := NewClient(mr)

	const modelID = "_feeder1"
	if _, err := c.GetCIMDictionary(context.Background(), modelID); err != nil {
		t.Fatalf("GetCIMDictionary: %v", err)
	}

	if mr.gotDestination != RequestConfig {
		t.Errorf("destination = %q, want %q", mr.gotDestination, RequestConfig)
	}
	body := decodeBody(t, mr.gotBody)
	if body["configurationType"] != "CIM Dictionary" {
		t.Errorf(`body["configurationType"] = %v, want "CIM Dictionary"`, body["configurationType"])
	}
	params, ok := body["parameters"].(map[string]any)
	if !ok {
		t.Fatalf(`body["parameters"] not a map: %v`, body["parameters"])
	}
	if params["model_id"] != modelID {
		t.Errorf(`parameters.model_id = %v, want %q`, params["model_id"], modelID)
	}
}

func TestGetCIMDictionaryParse(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"data":{"feeders":[{
		"energyconsumers":[{"mRID":"_ec1","name":"EC1","phases":"ABC"}],
		"capacitors":[{"mRID":"_cap1","name":"Cap1"}],
		"measurements":[{"mRID":"_m1","name":"M1"}],
		"solarpanels":[{"mRID":"_sp1","name":"SP1"}],
		"batteries":[{"mRID":"_b1","name":"B1"}]
	}]}}`)}
	c := NewClient(mr)

	res, err := c.GetCIMDictionary(context.Background(), "_feeder1")
	if err != nil {
		t.Fatalf("GetCIMDictionary: %v", err)
	}
	if len(res.Feeders) != 1 {
		t.Fatalf("len(Feeders) = %d, want 1", len(res.Feeders))
	}
	f := res.Feeders[0]
	if len(f.EnergyConsumers) != 1 || f.EnergyConsumers[0].MRID != "_ec1" {
		t.Errorf("EnergyConsumers = %v, want [{_ec1, EC1, ABC}]", f.EnergyConsumers)
	}
	if f.EnergyConsumers[0].Phases != "ABC" {
		t.Errorf("EnergyConsumers[0].Phases = %q, want ABC", f.EnergyConsumers[0].Phases)
	}
	if len(f.Measurements) != 1 || f.Measurements[0].MRID != "_m1" {
		t.Errorf("Measurements = %v", f.Measurements)
	}
	if len(f.SolarPanels) != 1 || f.SolarPanels[0].MRID != "_sp1" {
		t.Errorf("SolarPanels = %v", f.SolarPanels)
	}
	if len(f.Batteries) != 1 || f.Batteries[0].MRID != "_b1" {
		t.Errorf("Batteries = %v", f.Batteries)
	}
	if len(f.Capacitors) != 1 || f.Capacitors[0].MRID != "_cap1" {
		t.Errorf("Capacitors = %v", f.Capacitors)
	}
}

// ---------- GetPlatformStatus ----------

func TestGetPlatformStatusEnvelope(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{}`)}
	c := NewClient(mr)

	if _, err := c.GetPlatformStatus(context.Background()); err != nil {
		t.Fatalf("GetPlatformStatus: %v", err)
	}

	if mr.gotDestination != RequestPlatformStatus {
		t.Errorf("destination = %q, want %q", mr.gotDestination, RequestPlatformStatus)
	}
	body := decodeBody(t, mr.gotBody)
	for _, k := range []string{"applications", "services", "appInstances", "serviceInstances"} {
		v, ok := body[k]
		if !ok {
			t.Errorf("body[%q] missing", k)
			continue
		}
		if b, ok := v.(bool); !ok || !b {
			t.Errorf("body[%q] = %v, want true", k, v)
		}
	}
}

func TestGetPlatformStatusParse(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"applications":[{"id":"app1"}],"services":[],"appInstances":[],"serviceInstances":[]}`)}
	c := NewClient(mr)

	res, err := c.GetPlatformStatus(context.Background())
	if err != nil {
		t.Fatalf("GetPlatformStatus: %v", err)
	}
	if res == nil {
		t.Fatal("GetPlatformStatus returned nil result")
	}
	if res.Applications == nil {
		t.Error("Applications is nil; expected raw JSON")
	}
}

// ---------- error envelope, generic ----------

func TestQueryModelInfoErrorEnvelope(t *testing.T) {
	t.Parallel()

	mr := &mockRequester{resp: []byte(`{"error":"boom"}`)}
	c := NewClient(mr)

	_, err := c.QueryModelInfo(context.Background())
	if !errors.Is(err, ErrServerError) {
		t.Errorf("err = %v, want errors.Is ErrServerError", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("err = %q, want containing 'boom'", err)
	}
}
