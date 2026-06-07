//go:build conformance

package conformance

import (
	"fmt"
	"strings"
	"testing"
)

// TestZZPub400Cleanup is a throwaway maintenance routine (NOT part of the
// conformance suite proper): it inventories the test schema's objects and drops
// the orphan fixtures the suite accumulates across runs -- G<token>_* tables and
// GP<token> SQL packages, each run using a fresh per-process token (see
// runToken) so old-run objects are never reclaimed automatically. Use it to free
// PUB400 free-tier storage when CREATE TABLE starts failing SQL-904 / 57011.
//
// Run explicitly: go test -tags=conformance -run TestZZPub400Cleanup ./test/conformance
func TestZZPub400Cleanup(t *testing.T) {
	db := openDB(t)
	sch := strings.ToUpper(schema())

	// Inventory every G-prefixed object with its size, largest first, via
	// QSYS2.OBJECT_STATISTICS (also the storage diagnostic).
	inv := fmt.Sprintf(`SELECT OBJNAME, OBJTYPE, OBJSIZE
	                    FROM TABLE(QSYS2.OBJECT_STATISTICS('%s','*ALL')) X
	                    WHERE OBJNAME LIKE 'G%%'
	                    ORDER BY OBJSIZE DESC`, sch)
	rows, err := db.Query(inv)
	if err != nil {
		t.Fatalf("inventory query: %v", err)
	}
	type obj struct {
		name, typ string
		size      int64
	}
	var objs []obj
	var total int64
	for rows.Next() {
		var o obj
		if err := rows.Scan(&o.name, &o.typ, &o.size); err != nil {
			t.Fatalf("scan inventory: %v", err)
		}
		objs = append(objs, o)
		total += o.size
	}
	rows.Close()
	t.Logf("schema %s: %d G-prefixed objects, total %d bytes (%.1f MB)", sch, len(objs), total, float64(total)/1e6)
	for i, o := range objs {
		if i < 25 {
			t.Logf("  %-12s %-9s %12d", o.name, o.typ, o.size)
		}
	}

	// Every G-prefixed object in this dedicated test schema is a suite artifact
	// (tables, the *PGM programs the extended-dynamic package mechanism builds,
	// and *SQLPKG packages), each tagged with a per-run token so old-run objects
	// are never reclaimed automatically. Drop them all to reclaim storage. CL
	// deletes go through QSYS2.QCMDEXC.
	qcmd := func(cl string) error {
		_, err := db.Exec(fmt.Sprintf("CALL QSYS2.QCMDEXC('%s')", strings.ReplaceAll(cl, "'", "''")))
		return err
	}
	var dropped, reclaimed int64
	for _, o := range objs {
		if o.name == "" || o.name[0] != 'G' {
			continue
		}
		var err error
		switch o.typ {
		case "*FILE":
			if _, err = db.Exec(fmt.Sprintf("DROP TABLE %s.%s", sch, o.name)); err != nil {
				err = qcmd(fmt.Sprintf("DLTF FILE(%s/%s)", sch, o.name))
			}
		case "*SQLPKG":
			if _, err = db.Exec(fmt.Sprintf("DROP PACKAGE %s.%s", sch, o.name)); err != nil {
				err = qcmd(fmt.Sprintf("DLTSQLPKG SQLPKG(%s/%s)", sch, o.name))
			}
		case "*PGM":
			err = qcmd(fmt.Sprintf("DLTPGM PGM(%s/%s)", sch, o.name))
		case "*SQLUDT":
			// SQL user-defined / ARRAY types (CREATE TYPE) leave a *SQLUDT
			// object that DLTOBJ does not delete by that type name; DROP TYPE
			// is the reclaiming verb. Issue #68's array-parameter capture
			// creates one per run (CREATE TYPE G<token>... AS <elem> ARRAY[N]).
			if _, err = db.Exec(fmt.Sprintf("DROP TYPE %s.%s", sch, o.name)); err != nil {
				err = qcmd(fmt.Sprintf("DLTOBJ OBJ(%s/%s) OBJTYPE(*SQLUDT)", sch, o.name))
			}
		default:
			err = qcmd(fmt.Sprintf("DLTOBJ OBJ(%s/%s) OBJTYPE(%s)", sch, o.name, o.typ))
		}
		if err != nil {
			t.Logf("  drop %s (%s): %v", o.name, o.typ, err)
			continue
		}
		dropped++
		reclaimed += o.size
	}
	t.Logf("dropped %d/%d objects, reclaimed ~%d bytes (%.1f MB)", dropped, len(objs), reclaimed, float64(reclaimed)/1e6)
}
