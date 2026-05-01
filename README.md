# gridappsd-2030_5-go

Go bridge that wires the IEEE 2030.5 protocol surface
(github.com/GRIDAPPSD/ieee-2030_5-go) to the GridAPPS-D platform via
STOMP/ActiveMQ messaging.

## Status

v0.0.0 scaffold. The bridge is not yet wired. internal/cimstomp/ carries
a STOMP publisher ported from gotocim. internal/cim/ and internal/registry/
are placeholders.

## Layout

- cmd/bridge/: bridge binary entry point.
- internal/cimstomp/: STOMP publisher, configuration, and connection management.
- internal/cim/: CIM model client (placeholder).
- internal/registry/: mRID-to-LFDI registry (placeholder).

## Build

    go build ./...
    go vet ./...
    go test ./...

## License

See LICENSE.
