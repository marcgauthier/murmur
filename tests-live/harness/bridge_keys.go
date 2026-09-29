package harness

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/marcgauthier/spedsql/bridge"
)

func generateBridgeKeys(dir string) (BridgeKeyFiles, error) {
	var out BridgeKeyFiles
	if err := os.MkdirAll(dir, 0755); err != nil {
		return out, err
	}
	signer, err := bridge.GenerateSigningKey()
	if err != nil {
		return out, err
	}
	recipient, err := bridge.GenerateRecipientKey()
	if err != nil {
		return out, err
	}
	out = BridgeKeyFiles{
		Dir:              dir,
		SignerKeyFile:    filepath.Join(dir, "signer.key"),
		SignerPubFile:    filepath.Join(dir, "signer.pub"),
		RecipientKeyFile: filepath.Join(dir, "recipient.key"),
		RecipientPubFile: filepath.Join(dir, "recipient.pub"),
	}
	pub := recipient.Public()
	writes := map[string][]byte{
		out.SignerKeyFile:    signer.MarshalBinary(),
		out.SignerPubFile:    signer.Public(),
		out.RecipientKeyFile: recipient.MarshalBinary(),
		out.RecipientPubFile: pub[:],
	}
	for path, data := range writes {
		if err := os.WriteFile(path, data, 0600); err != nil {
			return out, fmt.Errorf("write %s: %w", path, err)
		}
	}
	return out, nil
}
