package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/complacentsee/goJTOpen/internal/wirelog"
)

// Annotated names for known database-server ReqRepIDs / function IDs.
var reqNames = map[uint16]string{
	0x1800: "PREPARE",
	0x1801: "DESCRIBE",
	0x1803: "PREPARE_DESCRIBE",
	0x1804: "OPEN_DESCRIBE",
	0x1805: "EXECUTE",
	0x1806: "EXECUTE_IMMEDIATE",
	0x1807: "COMMIT",
	0x1808: "ROLLBACK",
	0x180A: "CLOSE",
	0x180B: "FETCH",
	0x180D: "PREPARE_EXECUTE",
	0x180E: "OPEN_DESCRIBE_FETCH",
	0x180F: "CREATE_PACKAGE",
	0x1812: "EXECUTE_OPEN_DESCRIBE",
	0x1816: "RETRIEVE_LOB_DATA",
	0x1817: "WRITE_LOB_DATA",
	0x1819: "FREE_LOB",
	0x1D00: "CREATE_RPB",
	0x1D03: "CHANGE_RPB",
	0x1E00: "CHANGE_DESCRIPTOR",
	0x1E01: "DELETE_DESCRIPTOR",
	0x1F80: "SET_SQL_ATTRIBUTES",
	0x1F81: "RETRIEVE_SQL_ATTRIBUTES",
	0x1FFF: "END_CONVERSATION",
	0x2800: "DB_REPLY",
}

// Friendly names for code points we care about while decoding the
// LOB-bind path.
var cpNames = map[uint16]string{
	0x3801: "Statement Name",
	0x3802: "NLSS Sort Sequence?",
	0x3803: "Library Name",
	0x3804: "SQL Package Name",
	0x3805: "Statement Type",
	0x3806: "Naming Convention",
	0x3807: "Date Format",
	0x3808: "Parameter Marker Format",
	0x3809: "Open Attributes",
	0x380B: "Cursor Name",
	0x380C: "Blocking Factor",
	0x380E: "Translation",
	0x380F: "LOB Data (reply, RETRIEVE_LOB_DATA)",
	0x3810: "Current LOB Length (reply)",
	0x3811: "Parameter Marker Data (original)",
	0x3812: "Parameter Marker Block Indicator?",
	0x3813: "Package Threshold (deprecated)",
	0x3814: "Parameter Marker Block Indicator",
	0x3818: "LOB Locator Handle",
	0x3819: "Requested Size",
	0x381A: "Start Offset",
	0x381B: "Compression Indicator",
	0x381C: "LOB Allocate Locator Indicator",
	0x381D: "LOB Data (write)",
	0x381E: "Extended SQLDA descriptor",
	0x381F: "Extended SQLDA data",
	0x3821: "Return Current Length Indicator",
	0x3822: "LOB Truncation Indicator",
	0x3824: "Autocommit",
	0x3828: "LOB Column Index",
	0x3829: "Input Locator Type",
	0x3833: "Variable-length Field Compression",
	0x3834: "Buffer Size",
	0x382F: "Variable Marker Data",
}

func reqName(id uint16) string {
	if n, ok := reqNames[id]; ok {
		return fmt.Sprintf("0x%04X (%s)", id, n)
	}
	return fmt.Sprintf("0x%04X (?)", id)
}

func cpName(cp uint16) string {
	if n, ok := cpNames[cp]; ok {
		return fmt.Sprintf("0x%04X %s", cp, n)
	}
	return fmt.Sprintf("0x%04X (?)", cp)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: lobinspect <trace>")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	must(err)
	defer f.Close()
	frames, err := wirelog.ParseJTOpenTrace(f)
	must(err)

	for _, f := range wirelog.Consolidate(frames) {
		dir := "->"
		if f.Direction == wirelog.Received {
			dir = "<-"
		}
		fmt.Printf("\n========== %s connID=%d  total %d bytes ==========\n", dir, f.ConnID, len(f.Bytes))
		walk(f.Bytes)
	}
}

func walk(b []byte) {
	off := 0
	for off+20 <= len(b) {
		l := binary.BigEndian.Uint32(b[off : off+4])
		if l < 20 || off+int(l) > len(b) {
			fmt.Printf("    [bad DSS at offset %d, length=%d, remaining=%d]\n", off, l, len(b)-off)
			fmt.Println(hex.Dump(b[off:]))
			return
		}
		decodeOne(b[off : off+int(l)])
		off += int(l)
	}
	if off != len(b) {
		fmt.Printf("    [trailing %d bytes]\n", len(b)-off)
	}
}

func decodeOne(frame []byte) {
	hdrID := binary.BigEndian.Uint16(frame[4:6])
	srv := binary.BigEndian.Uint16(frame[6:8])
	csi := binary.BigEndian.Uint32(frame[8:12])
	corr := binary.BigEndian.Uint32(frame[12:16])
	tplLen := binary.BigEndian.Uint16(frame[16:18])
	rid := binary.BigEndian.Uint16(frame[18:20])

	fmt.Printf("--- DSS Length=%d HeaderID=0x%04X ServerID=0x%04X CSInst=%d Corr=%d TplLen=%d ReqRep=%s\n",
		len(frame), hdrID, srv, csi, corr, tplLen, reqName(rid))

	if tplLen != 20 {
		fmt.Printf("    [unexpected TplLen %d, raw payload follows]\n", tplLen)
		fmt.Println(hex.Dump(frame[20:]))
		return
	}
	// Bytes 20..39 = template (20 bytes). Then 40..end is CP list.
	if len(frame) >= 40 {
		ors := binary.BigEndian.Uint32(frame[20:24])
		// JT400 uses bytes 24..27 for "based on ORS handle" et al; we
		// dump a short summary.
		retOrs := binary.BigEndian.Uint16(frame[28:30])
		fillOrs := binary.BigEndian.Uint16(frame[30:32])
		basedOrs := binary.BigEndian.Uint16(frame[32:34])
		rpb := binary.BigEndian.Uint16(frame[34:36])
		pmd := binary.BigEndian.Uint16(frame[36:38])
		paramCount := binary.BigEndian.Uint16(frame[38:40])
		fmt.Printf("    Template: ORS=0x%08X RetOrs=%d FillOrs=%d BasedOrs=%d RPB=%d PMD=%d paramCount=%d\n",
			ors, retOrs, fillOrs, basedOrs, rpb, pmd, paramCount)
	}
	cpOff := 40
	for cpOff+6 <= len(frame) {
		ll := binary.BigEndian.Uint32(frame[cpOff : cpOff+4])
		cp := binary.BigEndian.Uint16(frame[cpOff+4 : cpOff+6])
		if ll < 6 || int(ll) > len(frame)-cpOff {
			fmt.Printf("    [bad CP at offset %d: LL=%d cp=0x%04X]\n", cpOff, ll, cp)
			fmt.Println(hex.Dump(frame[cpOff:]))
			return
		}
		data := frame[cpOff+6 : cpOff+int(ll)]
		dumpCP(cp, data)
		cpOff += int(ll)
	}
	if cpOff != len(frame) {
		fmt.Printf("    [trailing %d bytes]\n", len(frame)-cpOff)
	}
}

func dumpCP(cp uint16, data []byte) {
	tag := cpName(cp)
	switch cp {
	case 0x3818, 0x3819, 0x381A, 0x3828:
		// uint32 BE
		if len(data) == 4 {
			fmt.Printf("    CP %s = %d (0x%08X)\n", tag, binary.BigEndian.Uint32(data), binary.BigEndian.Uint32(data))
			return
		}
	case 0x381B, 0x3822, 0x3824, 0x381C, 0x3821, 0x3805, 0x3829:
		// 1-byte
		if len(data) == 1 {
			fmt.Printf("    CP %s = 0x%02X\n", tag, data[0])
			return
		}
	case 0x3806, 0x3807, 0x3814:
		// 2-byte short
		if len(data) == 2 {
			fmt.Printf("    CP %s = %d (0x%04X)\n", tag, binary.BigEndian.Uint16(data), binary.BigEndian.Uint16(data))
			return
		}
	case 0x381D:
		// LOB Data (write) — print summary, first/last 32 bytes
		fmt.Printf("    CP %s len=%d\n", tag, len(data))
		if len(data) > 0 {
			n := len(data)
			head := n
			if head > 32 {
				head = 32
			}
			fmt.Printf("      head: %s\n", hexCompact(data[:head]))
			if n > 64 {
				fmt.Printf("      tail: %s\n", hexCompact(data[n-32:]))
			}
		}
		return
	case 0x381E, 0x381F:
		fmt.Printf("    CP %s len=%d\n", tag, len(data))
		// Dump compactly
		fmt.Println(indent(hex.Dump(data), 6))
		return
	}
	fmt.Printf("    CP %s len=%d %s\n", tag, len(data), hexCompact(data))
	if len(data) > 16 {
		fmt.Println(indent(hex.Dump(data), 6))
	}
}

func hexCompact(b []byte) string {
	var sb strings.Builder
	for i, x := range b {
		if i > 0 && i%2 == 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%02x", x)
	}
	return sb.String()
}

func indent(s string, n int) string {
	pad := strings.Repeat(" ", n)
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = pad + l
	}
	return strings.Join(lines, "\n")
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
