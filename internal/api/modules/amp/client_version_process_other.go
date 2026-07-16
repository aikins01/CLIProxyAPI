//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package amp

import (
	"context"
	"fmt"
	"time"
)

func ampRunClientVersionProbe(context.Context, string, []string, []string, time.Duration) ([]byte, error) {
	return nil, fmt.Errorf("installed Amp version probe is unsupported on this platform")
}
