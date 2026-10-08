//go:build windows

package endurancechaos_test

import (
	"errors"
)

func dirFreeBytes(path string) (uint64, error) {
	return 0, errors.New("statfs not supported on windows")
}
