package replicateddb

// IsReadOnlyStatement reports whether q is a read-only statement. Unknown or
// write-capable statements return false (safe default: write path).
//
// External adapters (such as the optional HTTP service wrapper) use this to
// route statements: reads may serve the shared materialization directly,
// while anything else must go through ExecContext or an explicit transaction
// so change capture is never bypassed.
func IsReadOnlyStatement(q string) bool { return isReadOnlyStatement(q) }
