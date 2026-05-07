// Command smoketest exercises whatever the current goJTOpen milestone
// has wired up against a real IBM i. M1 (signon side): connect to
// the as-signon host server (port 8476), do the exchange-attributes
// + signon-info handshake, print server VRM / CCSID / sign-on dates.
// M1 (database side): then open a second connection to the
// as-database host server (port 8471), do the xchg-rand-seed +
// start-server handshake, print the database prestart-job name.
//
// Configuration (env vars):
//
//	PUB400_HOST  (default: pub400.com)
//	PUB400_PORT  (default: 8476 -- the as-signon port)
//	PUB400_DBPORT (default: 8471 -- the as-database port)
//	PUB400_USER  (required)
//	PUB400_PWD   (required)
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/complacentsee/goJTOpen/ebcdic"
	"github.com/complacentsee/goJTOpen/hostserver"
)

func main() {
	host := envOr("PUB400_HOST", "pub400.com")
	signonPort := envOr("PUB400_PORT", "8476")
	dbPort := envOr("PUB400_DBPORT", "8471")
	user, ok := requireEnv("PUB400_USER")
	if !ok {
		os.Exit(2)
	}
	pwd, ok := requireEnv("PUB400_PWD")
	if !ok {
		os.Exit(2)
	}

	signonAddr := net.JoinHostPort(host, signonPort)
	fmt.Fprintf(os.Stderr, "goJTOpen smoketest -> %s as %s\n", signonAddr, user)

	// --- Step 1: as-signon. ---
	conn := dialOrDie(signonAddr, "as-signon")
	defer conn.Close()

	xa, si, err := hostserver.SignOn(conn, user, pwd)
	if err != nil {
		fail("sign-on: %v", err)
	}

	fmt.Printf("server VRM:      0x%08X (V%dR%dM%d)\n",
		xa.ServerVersion,
		(xa.ServerVersion>>16)&0xFF,
		(xa.ServerVersion>>8)&0xFF,
		xa.ServerVersion&0xFF)
	fmt.Printf("ds level:        %d\n", xa.ServerLevel)
	fmt.Printf("password level:  %d\n", xa.PasswordLevel)
	jobName, _ := ebcdic.CCSID37.Decode(xa.JobName)
	fmt.Printf("signon job:      %s\n", jobName)

	fmt.Printf("RC:              %d\n", si.ReturnCode)
	fmt.Printf("server CCSID:    %d\n", si.ServerCCSID)
	fmt.Printf("current signon:  %s\n", si.CurrentSignonDate.Format(time.RFC3339))
	fmt.Printf("last signon:     %s\n", si.LastSignonDate.Format(time.RFC3339))
	fmt.Printf("password expiry: %s (warn %d days)\n",
		si.ExpirationDate.Format("2006-01-02"),
		si.PWDExpirationWarningDays)

	// --- Step 2: as-database. ---
	dbAddr := net.JoinHostPort(host, dbPort)
	fmt.Fprintf(os.Stderr, "goJTOpen smoketest -> %s (as-database)\n", dbAddr)
	dbConn := dialOrDie(dbAddr, "as-database")
	defer dbConn.Close()

	xs, ss, err := hostserver.StartDatabaseService(dbConn, user, pwd)
	if err != nil {
		fail("start db service: %v", err)
	}

	fmt.Printf("db pwd level:    %d\n", xs.PasswordLevel)
	fmt.Printf("db RC:           %d\n", ss.ReturnCode)
	dbJobName, _ := ebcdic.CCSID37.Decode(ss.JobName)
	fmt.Printf("db job:          %s\n", dbJobName)

	fmt.Fprintln(os.Stderr, "ok")
}

// dialOrDie connects to addr and applies a 30s deadline; on any
// error it calls fail(). label appears in error messages for clarity
// when both signon + database connections are in flight.
func dialOrDie(addr, label string) net.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := net.Dialer{Timeout: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		fail("dial %s (%s): %v", label, addr, err)
	}
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		conn.Close()
		fail("set deadline (%s): %v", label, err)
	}
	return conn
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func requireEnv(key string) (string, bool) {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "missing required env var %s\n", key)
		return "", false
	}
	return v, true
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: ")
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

