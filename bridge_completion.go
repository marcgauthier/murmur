package murmur

import "github.com/marcgauthier/murmur/ids"

// CompleteBridgeImport atomically records completion receipts and contiguous
// stream progress after all imported effects have committed. It preserves
// existing receipts and never decreases progress on replay. A storage failure
// is returned to the importer; completion must not be acknowledged on failure.
func (db *DB) CompleteBridgeImport(stream string, applied uint64, receipts []ids.TxID) error {
	if err := db.requireWrite(); err != nil {
		return err
	}
	return db.store.CompleteBridgeImport(stream, applied, receipts)
}
