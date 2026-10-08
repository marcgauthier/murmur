//go:build murmur_testhooks

package murmur

import (
	"fmt"
	"os"
)

// InstallSchemaStoreCrashForTest arms an exact schema durability-boundary
// process exit for the separately tagged live-test daemon. This symbol and
// its hook implementation are absent from normal production builds.
func InstallSchemaStoreCrashForTest(database *DB, phase string) error {
	if database == nil {
		return fmt.Errorf("nil database")
	}
	exit := func(code int) func() error {
		return func() error {
			os.Exit(code)
			return nil
		}
	}
	switch phase {
	case "before-store":
		database.crash = &crashHooks{beforeSchemaStore: exit(85)}
	case "after-store":
		database.crash = &crashHooks{afterSchemaStore: exit(86)}
	case "":
		return nil
	default:
		return fmt.Errorf("unknown schema crash phase %q", phase)
	}
	return nil
}
