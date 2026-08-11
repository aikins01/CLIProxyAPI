//go:build !darwin

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "amp-local-broker is supported on macOS only")
	os.Exit(1)
}
