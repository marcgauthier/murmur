package bridge

import (
	"bytes"
	"testing"
)

func TestKeyMarshalRoundTrip(t *testing.T) {
	signer, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseSignerKey(signer.MarshalBinary())
	if err != nil {
		t.Fatal(err)
	}
	if back.ID != signer.ID || !bytes.Equal(back.Public(), signer.Public()) {
		t.Fatal("signer round trip changed identity")
	}
	recipient, err := GenerateRecipientKey()
	if err != nil {
		t.Fatal(err)
	}
	rback, err := ParseRecipientKey(recipient.MarshalBinary())
	if err != nil {
		t.Fatal(err)
	}
	if rback.ID != recipient.ID || rback.Public() != recipient.Public() {
		t.Fatal("recipient round trip changed identity")
	}
	if _, err := ParseSignerKey([]byte{1, 2, 3}); err == nil {
		t.Fatal("short signer key accepted")
	}
	if _, err := ParseRecipientKey(make([]byte, 31)); err == nil {
		t.Fatal("short recipient key accepted")
	}
}
