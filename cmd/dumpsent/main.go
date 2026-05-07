// Command dumpsent prints all consolidated Sent frames from a
// JTOpen trace file as hex dumps. Tooling-only sibling of
// cmd/dumprecv.
package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/complacentsee/goJTOpen/internal/wirelog"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: dumpsent <trace>")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	frames, err := wirelog.ParseJTOpenTrace(f)
	if err != nil {
		log.Fatal(err)
	}
	n := 0
	for _, fr := range wirelog.Consolidate(frames) {
		if fr.Direction != wirelog.Sent {
			continue
		}
		n++
		fmt.Printf("--- sent #%d (%d bytes, connID=%d) ---\n", n, len(fr.Bytes), fr.ConnID)
		fmt.Println(hex.Dump(fr.Bytes))
	}
}
