package main

import (
	"os"
	"syscall"
)

// terminationSignals lists the signals that end the program with a clean
// up.
//
// SIGTERM is the signal Docker sends on "docker stop", on a restart and
// when a container is replaced. Without it in this list the Go runtime
// ends the process at once: the websocket is not closed, and the camera is
// not told to stop its stream to us. The camera then keeps that stream
// registered, and it can refuse the next request with "Number of Mobile
// App connections above limit".
func terminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
