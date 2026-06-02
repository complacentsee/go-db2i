package hostserver

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// TestExecuteImmediate_PostWriteReadFailureTagsRequestSent pins the
// fix for issue #29: a transport failure that strikes AFTER the
// EXECUTE_IMMEDIATE frame has reached the server (the reply read is
// what fails) must wrap ErrRequestSent so the driver layer can refuse
// to replay the non-idempotent write on a fresh connection.
//
// Wire setup: a net.Pipe stands in for the host-server socket. The
// "server" goroutine consumes the request frame (so the client's
// WriteFrame completes cleanly) and then closes its end. The client's
// reply read then sees io.EOF -- the post-send failure we care about.
func TestExecuteImmediate_PostWriteReadFailureTagsRequestSent(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	go func() {
		// Drain the request frame so the client's send succeeds, then
		// drop the connection to simulate a peer reset / crash between
		// EXECUTE reaching the server and the reply coming back.
		_, _, _ = ReadFrame(server)
		server.Close()
	}()

	done := make(chan error, 1)
	go func() {
		_, err := ExecuteImmediate(client, "INSERT INTO T VALUES (1)", 1)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ExecuteImmediate returned nil after post-write peer close; want an error")
		}
		if !errors.Is(err, ErrRequestSent) {
			t.Fatalf("post-write read failure not tagged ErrRequestSent: %v", err)
		}
		// The underlying transport cause must remain reachable so the
		// driver's isConnLevelErr still recognises it as conn-level.
		if !errors.Is(err, io.EOF) {
			t.Errorf("post-write failure lost its io.EOF cause: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteImmediate hung; expected a prompt post-write read failure")
	}
}

// TestExecuteImmediate_PreSendFailureNotTaggedRequestSent is the
// counterpart: when the EXECUTE_IMMEDIATE frame never reaches the
// server (the WriteFrame itself fails because the socket is already
// closed), the request did NOT commit, so the error must NOT carry
// ErrRequestSent -- leaving database/sql free to replay it safely.
func TestExecuteImmediate_PreSendFailureNotTaggedRequestSent(t *testing.T) {
	client, server := net.Pipe()
	// Close both ends up front so the very first write fails before
	// any byte reaches the server.
	server.Close()
	client.Close()

	_, err := ExecuteImmediate(client, "INSERT INTO T VALUES (1)", 1)
	if err == nil {
		t.Fatal("ExecuteImmediate returned nil after pre-send close; want an error")
	}
	if errors.Is(err, ErrRequestSent) {
		t.Fatalf("pre-send failure wrongly tagged ErrRequestSent (would block safe replay): %v", err)
	}
}
