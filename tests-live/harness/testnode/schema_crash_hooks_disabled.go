//go:build !murmur_testhooks

package main

import (
	"fmt"

	db "github.com/marcgauthier/murmur"
)

func installSchemaCrashForTest(_ *db.DB, phase string) error {
	if phase != "" {
		return fmt.Errorf("schema crash hooks require the murmur_testhooks build tag")
	}
	return nil
}
