//go:build windows

package pauseresume_test

import (
	"errors"
	"os"
)

func supportsPauseResume() bool {
	return false
}

func pauseProcess(proc *os.Process) error {
	return errors.New("SIGSTOP not supported on windows")
}

func resumeProcess(proc *os.Process) error {
	return errors.New("SIGCONT not supported on windows")
}
