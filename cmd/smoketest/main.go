// Command smoketest is the per-milestone manual demo: a tiny program
// that exercises whatever the current driver milestone has wired up
// against a real IBM i. M1 will make this connect, sign on, and print
// the server version.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "goJTOpen smoketest: not yet implemented (gated on M1 handshake)")
	os.Exit(2)
}
