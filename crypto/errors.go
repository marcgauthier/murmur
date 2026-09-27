package crypto

import "errors"

// Sentinels for at-rest encryption failures.
var (
	// ErrAuth indicates an authentication failure: wrong key, flipped
	// bytes, spliced records, or replayed epochs. Fail closed.
	ErrAuth = errors.New("crypto: authentication failure")
	// ErrCorrupt indicates structural corruption: bad magic, unknown
	// version, truncation, or malformed records. Fail closed.
	ErrCorrupt = errors.New("crypto: corrupt container")
	// ErrRotationTooSoon indicates a data-key rotation inside the minimum
	// interval.
	ErrRotationTooSoon = errors.New("crypto: rotation too soon")
)
