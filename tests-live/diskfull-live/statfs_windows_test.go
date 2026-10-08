//go:build windows

package diskfulllive_test

import (
	"errors"
)

func getFreeBytes(path string) (int64, error) {
	return 0, errors.New("statfs not supported on windows")
}
