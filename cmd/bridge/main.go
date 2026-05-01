// Command bridge will eventually wire IEEE 2030.5 to GridAPPS-D via cimstomp.
// At v0.0.0 it prints a version banner and exits.
package main

import (
	"fmt"
	"os"
)

const version = "0.0.0"

func main() {
	fmt.Fprintf(os.Stdout, "gridappsd-2030_5-go bridge %s\n", version)
	os.Exit(0)
}
