// Command smoketest exercises whatever the current goJTOpen milestone
// has wired up against a real IBM i. M1: connect to the as-signon
// host server, do the exchange-attributes + signon-info handshake,
// print server VRM / CCSID / sign-on dates, disconnect.
//
// Configuration (env vars):
//
//	PUB400_HOST  (default: pub400.com)
//	PUB400_PORT  (default: 8476 -- the as-signon port)
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
	port := envOr("PUB400_PORT", "8476")
	user, ok := requireEnv("PUB400_USER")
	if !ok {
		os.Exit(2)
	}
	pwd, ok := requireEnv("PUB400_PWD")
	if !ok {
		os.Exit(2)
	}

	addr := net.JoinHostPort(host, port)
	fmt.Fprintf(os.Stderr, "goJTOpen smoketest -> %s as %s\n", addr, user)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := net.Dialer{Timeout: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		fail("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		fail("set deadline: %v", err)
	}

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

	fmt.Fprintln(os.Stderr, "ok")
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

