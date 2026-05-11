// Command dumprecv prints all consolidated Received frames from a
// JTOpen trace file as hex dumps. Tooling-only, not part of the
// shipped driver.
package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/complacentsee/go-db2i/internal/wirelog"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: dumprecv <trace>")
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
	rcv := 0
	for _, fr := range wirelog.Consolidate(frames) {
		if fr.Direction != wirelog.Received {
			continue
		}
		rcv++
		fmt.Printf("--- recv #%d (%d bytes, connID=%d) ---\n", rcv, len(fr.Bytes), fr.ConnID)
		fmt.Println(hex.Dump(fr.Bytes))
	}
}
