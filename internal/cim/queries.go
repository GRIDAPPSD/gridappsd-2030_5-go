package cim

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidFeederID is returned by the Query* SPARQL wrappers when the
// supplied feederID is empty or contains characters that would break the
// SPARQL syntax (quotes, angle brackets, newlines, backslashes). The
// Python upstream's Queries.py performs raw %-formatting with no
// validation; this defensive check stops a malformed feederID from
// reaching the broker as a corrupted SPARQL string.
var ErrInvalidFeederID = errors.New("cim: invalid feeder ID")

// SPARQL query templates reproduced from the Python upstream's
// Queries.py (gridappsd-2030_5) with trailing whitespace before
// newlines normalized; SPARQL ignores it and the queries execute
// identically against the broker. Verify against Queries.py before
// edits. Each template carries a single %s placeholder where the
// original Python code substitutes feeder_id via %-formatting.
// Substitution at call time uses fmt.Sprintf.
//
// Source line numbers (Queries.py):
//
//	sparqlQuerySolar          : line 117 (def at 116)
//	sparqlQueryBattery        : line 158 (def at 157)
//	sparqlQueryInverter       : line 201 (def at 200)
//	sparqlQueryAllDERGroups   : line 236 (def at 235)
//
// QuerySynchronousMachine (Queries.py:85) is intentionally not
// reproduced; the bridge's v0 measurement path does not consume it. See
// GAGO-010 ticket for rationale.
const (
	sparqlQuerySolar = `# Solar - DistSolar
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/CIM100#>
    SELECT ?name ?bus ?ratedS ?ratedU ?ipu ?p ?q ?fdrid ?id (group_concat(distinct ?phs;separator="\n") as ?phases) WHERE {
    ?s r:type c:PhotovoltaicUnit.
    ?s c:IdentifiedObject.name ?name.
    ?s c:IdentifiedObject.mRID ?id.
    ?pec c:PowerElectronicsConnection.PowerElectronicsUnit ?s.
    # feeder selection options - if all commented out, query matches all feeders
    VALUES ?fdrid {"%s"}  # 123 bus
    #VALUES ?fdrid {"_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"}  # 13 bus
    #VALUES ?fdrid {"_5B816B93-7A5F-B64C-8460-47C17D6E4B0F"}  # 13 bus assets
    #VALUES ?fdrid {"_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3"}  # 8500 node
    #VALUES ?fdrid {"_67AB291F-DCCD-31B7-B499-338206B9828F"}  # J1
    #VALUES ?fdrid {"_9CE150A8-8CC5-A0F9-B67E-BBD8C79D3095"}  # R2 12.47 3
     ?pec c:Equipment.EquipmentContainer ?fdr.
     ?fdr c:IdentifiedObject.mRID ?fdrid.
     #?pec c:IdentifiedObject.mRID ?id.
      #bind(strafter(str(?fdridraw), "_") as ?fdrid).
     ?pec c:PowerElectronicsConnection.ratedS ?ratedS.
     ?pec c:PowerElectronicsConnection.ratedU ?ratedU.
     ?pec c:PowerElectronicsConnection.maxIFault ?ipu.
     ?pec c:PowerElectronicsConnection.p ?p.
     ?pec c:PowerElectronicsConnection.q ?q.
     OPTIONAL {?pecp c:PowerElectronicsConnectionPhase.PowerElectronicsConnection ?pec.
     ?pecp c:PowerElectronicsConnectionPhase.phase ?phsraw.
       bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
     #bind(strafter(str(?s),"#_") as ?id).
     ?t c:Terminal.ConductingEquipment ?pec.
     ?t c:Terminal.ConnectivityNode ?cn.
     ?cn c:IdentifiedObject.name ?bus
    }
    GROUP by ?name ?bus ?ratedS ?ratedU ?ipu ?p ?q ?fdrid ?id
    ORDER by ?name
    `

	sparqlQueryBattery = `# Storage - DistStorage
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/CIM100#>
    SELECT ?name ?bus ?ratedS ?ratedU ?ipu ?ratedE ?storedE ?state ?p ?q ?id ?fdrid (group_concat(distinct ?phs;separator="\n") as ?phases) WHERE {
     ?s r:type c:BatteryUnit.
     ?s c:IdentifiedObject.name ?name.
     ?pec c:PowerElectronicsConnection.PowerElectronicsUnit ?s.
    # feeder selection options - if all commented out, query matches all feeders
    VALUES ?fdrid {"%s"}  # 123 bus
    #VALUES ?fdrid {"_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"}  # 13 bus
    #VALUES ?fdrid {"_5B816B93-7A5F-B64C-8460-47C17D6E4B0F"}  # 13 bus assets
    #VALUES ?fdrid {"_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3"}  # 8500 node
    #VALUES ?fdrid {"_67AB291F-DCCD-31B7-B499-338206B9828F"}  # J1
    #VALUES ?fdrid {"_9CE150A8-8CC5-A0F9-B67E-BBD8C79D3095"}  # R2 12.47 3
     ?pec c:Equipment.EquipmentContainer ?fdr.
     ?fdr c:IdentifiedObject.mRID ?fdrid.
      #bind(strafter(str(?fdridraw), "_") as ?fdrid).
     ?pec c:PowerElectronicsConnection.ratedS ?ratedS.
     ?pec c:PowerElectronicsConnection.ratedU ?ratedU.
     ?pec c:PowerElectronicsConnection.maxIFault ?ipu.
     ?s c:BatteryUnit.ratedE ?ratedE.
     ?s c:BatteryUnit.storedE ?storedE.
     ?s c:BatteryUnit.batteryState ?stateraw.
       bind(strafter(str(?stateraw),"BatteryState.") as ?state)
     ?pec c:PowerElectronicsConnection.p ?p.
     ?pec c:PowerElectronicsConnection.q ?q.
     OPTIONAL {?pecp c:PowerElectronicsConnectionPhase.PowerElectronicsConnection ?pec.
     ?pecp c:PowerElectronicsConnectionPhase.phase ?phsraw.
       bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
     bind(strafter(str(?s),"#_") as ?id).
     ?t c:Terminal.ConductingEquipment ?pec.
     ?t c:Terminal.ConnectivityNode ?cn.
     ?cn c:IdentifiedObject.name ?bus
    }
    GROUP by ?name ?bus ?ratedS ?ratedU ?ipu ?ratedE ?storedE ?state ?p ?q ?id ?fdrid
    ORDER by ?name
    `

	sparqlQueryInverter = `
    PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c: <http://iec.ch/TC57/CIM100#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?name ?bus ?ratedS ?ratedU ?ipu ?p ?q ?fdrid ?id ?pecid (group_concat(distinct ?phs;separator="\n") as ?phases)  WHERE {
    VALUES ?fdrid {"%s"}
    #?s r:type c:PhotovoltaicUnit.
    #?s r:type c:BatteryUnit.
    ?s c:IdentifiedObject.name ?name.
    ?s c:IdentifiedObject.mRID ?id.
    ?pec c:PowerElectronicsConnection.PowerElectronicsUnit ?s.
    ?pec c:IdentifiedObject.mRID ?pecid.
    ?pec c:Equipment.EquipmentContainer ?fdr.
    ?fdr c:IdentifiedObject.mRID ?fdrid.
    ?pec c:PowerElectronicsConnection.ratedS ?ratedS.
    ?pec c:PowerElectronicsConnection.ratedU ?ratedU.
    ?pec c:PowerElectronicsConnection.maxIFault ?ipu.
    ?pec c:PowerElectronicsConnection.p ?p.
    ?pec c:PowerElectronicsConnection.q ?q.
    OPTIONAL {?pecp c:PowerElectronicsConnectionPhase.PowerElectronicsConnection ?pec.
    ?pecp c:PowerElectronicsConnectionPhase.phase ?phsraw.
    bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
    ?t c:Terminal.ConductingEquipment ?pec.
    ?t c:Terminal.ConnectivityNode ?cn.
    ?cn c:IdentifiedObject.name ?bus
    }
    GROUP by ?name ?bus ?ratedS ?ratedU ?ipu ?p ?q ?fdrid ?id ?pecid
    ORDER by ?name
    `

	sparqlQueryAllDERGroups = `#get all EndDeviceGroup
    PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
    PREFIX  r:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX  c:    <http://iec.ch/TC57/CIM100#>
    select ?mRID ?description (group_concat(distinct ?name;separator="\n") as ?names)
                              (group_concat(distinct ?device;separator="\n") as ?devices)
                              (group_concat(distinct ?func;separator="\n") as ?funcs)
    VALUES ?fdrid {"%s"}
    where {
      ?q1 a c:EndDeviceGroup .
      ?q1 c:IdentifiedObject.mRID ?mRIDraw .
        bind(strafter(str(?mRIDraw), "_") as ?mRID).
      ?q1 c:IdentifiedObject.name ?name .
      ?q1 c:IdentifiedObject.description ?description .
      Optional{
        ?q1 c:EndDeviceGroup.EndDevice ?deviceobj .
        ?deviceobj c:IdentifiedObject.mRID ?deviceID .
        ?deviceobj c:IdentifiedObject.name ?deviceName .
        ?deviceobj c:EndDevice.isSmartInverter ?isSmart .
        bind(concat(strafter(str(?deviceID), "_"), ",", str(?deviceName), ",", str(?isSmart)) as ?device)
      }
      ?q1 c:DERFunction ?derFunc .
      ?derFunc ?pfunc ?vfuc .
      Filter(?pfunc !=r:type)
        bind(concat(strafter(str(?pfunc), "DERFunction."), ",", str(?vfuc)) as ?func)
    }
    Group by ?mRID ?description
    Order by ?mRID
    `
)

// validateFeederID rejects empty IDs and IDs containing characters that
// would break the SPARQL VALUES clause when interpolated. CIM mRIDs are
// well-shaped (UUID-ish, optionally underscore-prefixed) at production
// call sites, so this check is conservative; it exists to prevent a
// misuse from silently producing a malformed query.
func validateFeederID(feederID string) error {
	if feederID == "" {
		return fmt.Errorf("%w: empty", ErrInvalidFeederID)
	}
	if strings.ContainsAny(feederID, "\"<>\n\r\\") {
		return fmt.Errorf("%w: contains forbidden character", ErrInvalidFeederID)
	}
	return nil
}

// queryFeederTemplate validates the feederID, substitutes it into the
// SPARQL template, and dispatches through Client.QueryData. The four
// public Query* wrappers differ only in the template they pass.
func (c *Client) queryFeederTemplate(ctx context.Context, template, feederID string) (*QueryDataResult, error) {
	if err := validateFeederID(feederID); err != nil {
		return nil, err
	}
	sparql := fmt.Sprintf(template, feederID)
	return c.QueryData(ctx, sparql)
}

// QuerySolar runs the photovoltaic-unit enumeration SPARQL from
// gridappsd-2030_5 Queries.py:117 against the powergrid-model service,
// scoped to feederID. The returned QueryDataResult exposes the raw
// SPARQL bindings; callers are responsible for projecting them into
// domain types and may use Binding.AsJSONLD on JSON-LD-shaped values.
func (c *Client) QuerySolar(ctx context.Context, feederID string) (*QueryDataResult, error) {
	return c.queryFeederTemplate(ctx, sparqlQuerySolar, feederID)
}

// QueryBattery runs the battery-unit enumeration SPARQL from
// gridappsd-2030_5 Queries.py:158 against the powergrid-model service,
// scoped to feederID. See QuerySolar for the binding-shape contract.
func (c *Client) QueryBattery(ctx context.Context, feederID string) (*QueryDataResult, error) {
	return c.queryFeederTemplate(ctx, sparqlQueryBattery, feederID)
}

// QueryInverter runs the generic PowerElectronicsConnection enumeration
// SPARQL from gridappsd-2030_5 Queries.py:201 against the powergrid-
// model service, scoped to feederID. The Python upstream comments out
// the unit-type filter so the query covers any inverter; this Go
// reproduction keeps that behavior.
func (c *Client) QueryInverter(ctx context.Context, feederID string) (*QueryDataResult, error) {
	return c.queryFeederTemplate(ctx, sparqlQueryInverter, feederID)
}

// QueryAllDERGroups runs the EndDeviceGroup enumeration SPARQL from
// gridappsd-2030_5 Queries.py:236 against the powergrid-model service,
// scoped to feederID. The result includes group-concatenated name,
// device, and DERFunction lists per group.
func (c *Client) QueryAllDERGroups(ctx context.Context, feederID string) (*QueryDataResult, error) {
	return c.queryFeederTemplate(ctx, sparqlQueryAllDERGroups, feederID)
}
