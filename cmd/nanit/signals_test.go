package main

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestTerminationSignalsIncludeDockerStop(t *testing.T) {
	want := map[os.Signal]bool{os.Interrupt: false, syscall.SIGTERM: false}

	for _, sig := range terminationSignals() {
		if _, ok := want[sig]; ok {
			want[sig] = true
		}
	}

	for sig, found := range want {
		if !found {
			t.Errorf("%v does not end the program with a clean up", sig)
		}
	}
}

// The program must receive SIGTERM itself. If the signal is not
// registered, the Go runtime ends the process, and this test with it.
func TestSIGTERMIsDeliveredToTheProgram(t *testing.T) {
	received := make(chan os.Signal, 1)
	signal.Notify(received, terminationSignals()...)
	defer signal.Stop(received)

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("unable to send SIGTERM to this process: %v", err)
	}

	select {
	case sig := <-received:
		if sig != syscall.SIGTERM {
			t.Fatalf("received %v, want SIGTERM", sig)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SIGTERM was not delivered")
	}
}
